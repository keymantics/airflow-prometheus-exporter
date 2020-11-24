# Airflow Prometheus Exporter

The Airflow Prometheus Exporter exposes various metrics about the Scheduler, DAGs and Tasks which helps improve the
observability of an Airflow cluster.

The exporter is based on this [prometheus exporter for Airflow](https://github.com/epoch8/airflow-exporter) and this [one](https://github.com/robinhood/airflow-prometheus-exporter).

## Requirements

The plugin has been tested with:

- Airflow >= 1.10.3
- Python 3.6+

The scheduler metrics assume that there is a DAG named `canary_dag`. In our setup, the `canary_dag` is a DAG which has a
tasks which perform very simple actions such as establishing database connections. This DAG is used to test the uptime
of the Airflow scheduler itself.

## Installation

The exporter can be installed as an Airflow Plugin using:

```bash
pip install airflow-prometheus-exporter
```

This should ideally be installed in your Airflow virtualenv.

## Metrics

Metrics will be available at:

`http://<your_airflow_host_and_port>/admin/metrics/`

### Task Specific Metrics

#### `airflow_task_status`

Number of tasks with a specific status.

All the possible states are listed [here](https://github.com/apache/airflow/blob/master/airflow/utils/state.py#L46).

#### `airflow_task_duration`

Duration of running tasks in seconds.

#### `airflow_last_task_success_time`

Last time in seconds since last task success.

#### `airflow_task_fail_count`

Number of times a particular task has failed.

### XCOM Specific Metrics

#### `airflow_xcom_parameter`

Value of configurable parameter from XCOM table.

XCOM values are identified by a `task_id` and an optional `xcom_key` (defaults to 'return_value').
Each XCOM value must be a dictionary. If `key` is found in this dictionary, the parameter is extracted and reported as a gauge.

Add `task_id`, `xcom_key` and `key` combinations in config.yaml:

```yaml
xcom_params:
  -
    task_id: abc
    key: record_count
  -
    task_id: def
    xcom_key: prometheus_metrics
    key: errors
```

The special `task_id` value 'all' will match against all airflow tasks:

```yaml
xcom_params:
 -
    task_id: all
    key: record_count
```

The special `key` value 'all' will match against all parameters inside Xcom value.

```yaml
xcom_params:
 -
    task_id: all
    xcom_key: prometheus_metrics
    key: all
```

To generate XCOM values for default key 'return_value', simply return from your dag
```python
return {'record_count': 123}
```

Custom XCOM key can also be used. From your DAG, export the XCOM value like:
```python
kwargs['ti'].xcom_push(key='prometheus_metrics', value={'errors': 2, 'data_size': 1024})
```

### Dag Specific Metrics

#### `airflow_dag_status`

Number of DAGs with a specific status.

All the possible states are listed [here](https://github.com/apache/airflow/blob/master/airflow/utils/state.py#L59)

#### `airflow_dag_run_duration`

Duration of running DagRun in seconds.

#### `airflow_last_dag_success_time`

Last time in seconds since last DAG success.

### Scheduler Metrics

#### `airflow_dag_scheduler_delay`

Scheduling delay for a DAG Run in seconds. This metric assumes there is a `canary_dag`.

The scheduling delay is measured as the delay between when a DAG is marked as `SCHEDULED` and when it actually starts
`RUNNING`.

#### `airflow_task_scheduler_delay`

Scheduling delay for a Task in seconds. This metric assumes there is a `canary_dag`.

#### `airflow_num_queued_tasks`

Number of tasks in the `QUEUED` state at any given instance.
