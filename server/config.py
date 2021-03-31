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
    HOST_NAME = get_env("HOST_NAME")
    PORT = get_env("PORT")
    USER = get_env("USER")
    AUTH = get_env("AUTH")
    PASSWORD = get_env("PASSWORD")


class DevelopmentConfig(BaseConfig):
    FLASK_DEBUG = True


class ProductionConfig(BaseConfig):
    FLASK_DEBUG = False
