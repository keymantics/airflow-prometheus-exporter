import os
from pathlib import Path
import yaml


CONFIG_FILENAME = 'config.yaml'


def load_xcom_config():
    """Loads the XCom config if present."""
    # Start searching in current working directory,
    # fallback to local config file.
    search_paths = [
        Path.cwd(),
        os.path.dirname(os.path.abspath(__file__))
    ]

    full_path = None
    for path in search_paths:
        full_path = os.path.join(path, CONFIG_FILENAME)
        if os.path.isfile(full_path):
            break
        else:
            full_path = None

    if not full_path:
        return {}

    try:
        with open(full_path) as file:
            # The FullLoader parameter handles the conversion from YAML
            # scalar values to Python the dictionary format
            return yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        return {}
