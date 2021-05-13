import os
from typing import Any

LOG_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s in %(module)s [%(processName)s]: %(message)s",
        }
    },
    "handlers": {
        "wsgi": {
            "class": "logging.StreamHandler",
            "stream": "ext://flask.logging.wsgi_errors_stream",
            "formatter": "default",
        }
    },
    "root": {"level": "INFO", "handlers": ["wsgi"]},
}


class MissingEnvironmentVariable(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


def get_env(env: str, default_value: Any = None) -> Any:
    try:
        return os.environ.get(env)
    except KeyError:
        if default_value is not None:
            return default_value
        raise MissingEnvironmentVariable(f"{env} does not exist")


class BaseConfig:
    HIVE_HOST_NAME = get_env("HIVE_HOST_NAME")
    HIVE_PORT = get_env("HIVE_PORT")
    SCHEDULER_TIMEOUT_MINUTES = get_env("SCHEDULER_TIMEOUT_MINUTES", 1)


class DevelopmentConfig(BaseConfig):
    FLASK_DEBUG = True


class ProductionConfig(BaseConfig):
    FLASK_DEBUG = False
