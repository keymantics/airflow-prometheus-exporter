"""Prometheus exporter for Airflow."""
import json
import pickle
from contextlib import contextmanager

from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskFail, TaskInstance, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import RBAC, Session
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.log.logging_mixin import LoggingMixin
from flask import Response
from flask_admin import BaseView, expose
from sqlalchemy import and_, func

from airflow_prometheus_exporter.xcom_config import load_xcom_config

CANARY_DAG = "canary_dag"


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()


######################
# DAG Related Metrics
######################


def get_dag_state_info():
    """Number of DAG Runs with particular state."""
    with session_scope(Session) as session:
        dag_status_query = (
            session.query(
                DagRun.dag_id,
                DagRun.state,
                func.count(DagRun.state).label("count"),
        ).group_by(DagRun.dag_id, DagRun.state).subquery()

)
        return (
            session.query(
                dag_status_query.c.dag_id,
                dag_status_query.c.state,
                dag_status_query.c.count,
                DagModel.owners,
            )
            .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .all()
        )


def get_dag_duration_info(state):
    """Duration of successful DAG Runs."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == state,
                DagRun.end_date.isnot(None) if state == State.SUCCESS else True,
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        dag_start_dt_query = (
            session.query(
                max_execution_dt_query.c.dag_id,
                max_execution_dt_query.c.max_execution_dt.label(
                    "execution_date"
                ),
                func.min(TaskInstance.start_date).label("start_date"),
            )
            .filter(
            TaskInstance.start_date.isnot(None)
        ).join(
            TaskInstance,
            and_(
                TaskInstance.dag_id == max_execution_dt_query.c.dag_id,
                (
                    TaskInstance.execution_date ==
                    max_execution_dt_query.c.max_execution_dt
                ),
                ),
            )
            .group_by(
                max_execution_dt_query.c.dag_id,
                max_execution_dt_query.c.max_execution_dt,
            )
            .subquery()
        )

        return (
            session.query(
                dag_start_dt_query.c.dag_id,
                dag_start_dt_query.c.start_date,
                DagRun.end_date,
            )
            .join(
                DagRun,
                and_(
                    DagRun.dag_id == dag_start_dt_query.c.dag_id,
                    DagRun.execution_date
                    == dag_start_dt_query.c.execution_date,
                ),
            )
            .filter(
                TaskInstance.start_date.isnot(None),
                TaskInstance.end_date.isnot(None),
            )
            .all()
        )


######################
# Task Related Metrics
######################


def get_task_state_info():
    """Number of task instances with particular state."""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.state,
                func.count(TaskInstance.dag_id).label("value"),
            )
            .group_by(
                TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state
            )
            .subquery()
        )
        return (
            session.query(
                task_status_query.c.dag_id,
                task_status_query.c.task_id,
                task_status_query.c.state,
                task_status_query.c.value,
                DagModel.owners,
            )
            .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .all()
        )


def get_task_failure_counts():
    """Compute Task Failure Counts."""
    with session_scope(Session) as session:
        return (
            session.query(
                TaskFail.dag_id,
                TaskFail.task_id,
                func.count(TaskFail.dag_id).label("count"),
            )
            .join(DagModel, DagModel.dag_id == TaskFail.dag_id,)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .group_by(TaskFail.dag_id, TaskFail.task_id,)
        )


def get_xcom_params(task_id):
    """XCom parameters for matching task_id's for the latest run of a DAG."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        query = session.query(XCom.dag_id, XCom.task_id, XCom.value, XCom.key).join(
            max_execution_dt_query,
            and_(
                (XCom.dag_id == max_execution_dt_query.c.dag_id),
                (
                    XCom.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        if task_id == "all":
            return query.all()
        else:
            return query.filter(XCom.task_id == task_id).all()


def extract_xcom_parameter(value):
    """Deserializes value stored in xcom table."""
    enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
    if enable_pickling:
        value = pickle.loads(value)
        try:
            value = json.loads(value)
            return value
        except Exception:
            return {}
    else:
        try:
            return json.loads(value.decode("UTF-8"))
        except ValueError as err:
            log = LoggingMixin().log
            log.error(
                "Could not deserialize the XCOM value from JSON. "
                "If you are using pickles instead of JSON "
                "for XCOM, then you need to enable pickle "
                f"support for XCOM in your airflow config. : {err}"
            )
            return {}


def get_task_duration_info(state):
    """Duration of successful tasks in seconds."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id,)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == state,
                DagRun.end_date.isnot(None) if state == State.SUCCESS else True,
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        task_duration_query = session.query(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            func.max(TaskInstance.execution_date).label('max_execution_dt')
        ).filter(
            TaskInstance.state == state,
            TaskInstance.start_date.isnot(None),
            TaskInstance.end_date.isnot(None) if state == State.SUCCESS else True,
        ).group_by(
            TaskInstance.dag_id,
            TaskInstance.task_id
        ).subquery()

        task_latest_execution_dt = session.query(
            task_duration_query.c.dag_id,
            task_duration_query.c.task_id,
            task_duration_query.c.max_execution_dt.label('execution_date'),
        ).join(
            max_execution_dt_query,
            and_(
                (
                    task_duration_query.c.dag_id ==
                    max_execution_dt_query.c.dag_id
                ),
                (
                    task_duration_query.c.max_execution_dt ==
                    max_execution_dt_query.c.max_execution_dt
                ),
            )
        ).subquery()

        return session.query(
            task_latest_execution_dt.c.dag_id,
            task_latest_execution_dt.c.task_id,
            TaskInstance.start_date,
            TaskInstance.end_date,
            task_latest_execution_dt.c.execution_date,
        ).join(
            TaskInstance,
            and_(
                TaskInstance.dag_id == task_latest_execution_dt.c.dag_id,
                TaskInstance.task_id == task_latest_execution_dt.c.task_id,
                (
                    TaskInstance.execution_date ==
                    task_latest_execution_dt.c.execution_date
                ),
            )
            .filter(
                TaskInstance.state == State.SUCCESS,
                TaskInstance.start_date.isnot(None),
                TaskInstance.end_date.isnot(None),
            )
            .all()
        )


######################
# Scheduler Related Metrics
######################


def get_dag_scheduler_delay():
    """Compute DAG scheduling delay."""
    with session_scope(Session) as session:
        return (
            session.query(
                DagRun.dag_id, DagRun.execution_date, DagRun.start_date,
            )
            .filter(DagRun.dag_id == CANARY_DAG,)
            .order_by(DagRun.execution_date.desc())
            .limit(1)
            .all()
        )


def get_task_scheduler_delay():
    """Compute Task scheduling delay."""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.queue,
                func.max(TaskInstance.start_date).label("max_start"),
            )
            .filter(
                TaskInstance.dag_id == CANARY_DAG,
                TaskInstance.queued_dttm.isnot(None),
            )
            .group_by(TaskInstance.queue)
            .subquery()
        )

        return (
            session.query(
                task_status_query.c.queue,
                TaskInstance.execution_date,
                TaskInstance.queued_dttm,
                task_status_query.c.max_start.label("start_date"),
            )
            .join(
                TaskInstance,
                and_(
                    TaskInstance.queue == task_status_query.c.queue,
                    TaskInstance.start_date == task_status_query.c.max_start,
                ),
            )
            .filter(
                TaskInstance.dag_id
                == CANARY_DAG,  # Redundant, for performance.
            )
            .all()
        )


def get_num_queued_tasks():
    """Number of queued tasks currently."""
    with session_scope(Session) as session:
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )


class MetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
        utc_now = timezone.utcnow()

        # Task metrics
        t_state = GaugeMetricFamily(
            "airflow_task_status",
            "Shows the number of task instances with particular status",
            labels=["dag_id", "task_id", "owner", "status"],
        )

        tasks_info_by_id = {}
        for task in get_task_state_info():
            task_uid = f'{task.dag_id}_{task.task_id}'
            task_info = tasks_info_by_id.setdefault(task_uid, {
                'meta': task,
                'state_count': {}
            })
            state = task.state or 'none'
            task_info['state_count'][state] = task.count

        for task_info in tasks_info_by_id.values():
            task = task_info['meta']
            for state in State.task_states:
                state = state or 'none'
                t_state.add_metric(
                    [task.dag_id, task.task_id, task.owners, state],
                    task_info['state_count'].get(state, 0)
                )

        yield t_state

        task_duration = GaugeMetricFamily(
            "airflow_task_duration",
            "Duration of running tasks in seconds",
            labels=["task_id", "dag_id", "execution_date"],
        )

        for task in get_task_duration_info(State.RUNNING):
            task_duration_value = (
                utc_now - task.start_date
            ).total_seconds()
            task_duration.add_metric(
                [task.task_id, task.dag_id, str(task.execution_date.date())],
                task_duration_value,
            )

        yield task_duration

        last_task_success_time = GaugeMetricFamily(
            'airflow_last_task_success_time',
            'Elapsed time in seconds since last task success',
            labels=['task_id', 'dag_id', 'execution_date']
        )

        for task in get_task_duration_info(State.SUCCESS):
            last_task_success_time_value = (utc_now - task.end_date).total_seconds()
            last_task_success_time.add_metric(
                [task.task_id, task.dag_id, str(task.execution_date.date())],
                last_task_success_time_value
            )

        yield last_task_success_time

        task_failure_count = GaugeMetricFamily(
            "airflow_task_fail_count",
            "Count of failed tasks",
            labels=["dag_id", "task_id"],
        )

        for task in get_task_failure_counts():
            task_failure_count.add_metric(
                [task.dag_id, task.task_id], task.count
            )

        yield task_failure_count

        # Dag Metrics
        d_state = GaugeMetricFamily(
            "airflow_dag_status",
            "Shows the number of dag starts with this status",
            labels=["dag_id", "owner", "status"],
        )

        dags_info_by_id = {}
        for dag in get_dag_state_info():
            dag_info = dags_info_by_id.setdefault(dag.dag_id, {
                'meta': dag,
                'state_count': {}
            })
            dag_info['state_count'][dag.state] = dag.count

        for dag_info in dags_info_by_id.values():
            dag = dag_info['meta']
            for state in State.dag_states:
                d_state.add_metric(
                    [dag.dag_id, dag.owners, state],
                    dag_info['state_count'].get(state, 0)
                )
        yield d_state

        dag_duration = GaugeMetricFamily(
            "airflow_dag_run_duration",
            "Duration of running dag_runs in seconds",
            labels=["dag_id"],
        )

        for dag in get_dag_duration_info(State.RUNNING):
            dag_duration_value = (
                utc_now - dag.start_date).total_seconds()
            dag_duration.add_metric(
                [dag.dag_id],
                dag_duration_value
            )
        yield dag_duration

        last_dag_success_time = GaugeMetricFamily(
            'airflow_last_dag_success_time',
            'Elapsed time in seconds since last DAG success',
            labels=['dag_id']
        )

        for dag in get_dag_duration_info(State.SUCCESS):
            last_dag_success_time_value = (utc_now - dag.end_date).total_seconds()
            last_dag_success_time.add_metric(
                [dag.dag_id],
                last_dag_success_time_value
            )

        yield last_dag_success_time

        # Scheduler Metrics
        dag_scheduler_delay = GaugeMetricFamily(
            "airflow_dag_scheduler_delay",
            "Airflow DAG scheduling delay",
            labels=["dag_id"],
        )

        for dag in get_dag_scheduler_delay():
            dag_scheduling_delay_value = (dag.start_date - dag.execution_date
            ).total_seconds()
            dag_scheduler_delay.add_metric(
                [dag.dag_id], dag_scheduling_delay_value
            )

        yield dag_scheduler_delay

        # XCOM parameters

        xcom_params = GaugeMetricFamily(
            "airflow_xcom_parameter",
            "Airflow Xcom Parameter",
            labels=["dag_id", "task_id", "xcom_key"],
        )

        xcom_config = load_xcom_config()
        for tasks in xcom_config.get("xcom_params", []):
            for param in get_xcom_params(tasks["task_id"]):
                xcom_value = extract_xcom_parameter(param.value)

                xcom_params.add_metric(
                    [param.dag_id, param.task_id, param.key], xcom_value
                )

        yield xcom_params

        task_scheduler_delay = GaugeMetricFamily(
            "airflow_task_scheduler_delay",
            "Airflow Task scheduling delay",
            labels=["queue"],
        )

        for task in get_task_scheduler_delay():
            task_scheduling_delay_value = (
                task.start_date - task.queued_dttm
            ).total_seconds()
            task_scheduler_delay.add_metric(
                [task.queue], task_scheduling_delay_value
            )

        yield task_scheduler_delay

        num_queued_tasks_metric = GaugeMetricFamily(
            "airflow_num_queued_tasks", "Airflow Number of Queued Tasks",
        )

        num_queued_tasks = get_num_queued_tasks()
        num_queued_tasks_metric.add_metric([], num_queued_tasks)

        yield num_queued_tasks_metric


REGISTRY.register(MetricsCollector())

if RBAC:
    from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose

    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"

        @FABexpose('/')
        def list(self):
            return Response(generate_latest(), mimetype='text')

    # Metrics View for Flask app builder used in airflow with rbac enabled
    RBACmetricsView = {
        "view": RBACMetrics(),
        "name": "Metrics",
        "category": "Public"
    }
    ADMIN_VIEW = []
    RBAC_VIEW = [RBACmetricsView]
else:
    class Metrics(BaseView):
        @expose("/")
        def index(self):
            return Response(generate_latest(), mimetype="text/plain")

    ADMIN_VIEW = [Metrics(category="Prometheus exporter", name="Metrics")]
    RBAC_VIEW = []


class AirflowPrometheusPlugin(AirflowPlugin):
    """Airflow Plugin for collecting metrics."""

    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = ADMIN_VIEW
    flask_blueprints = []
    menu_links = []
    appbuilder_views = RBAC_VIEW
    appbuilder_menu_items = []
