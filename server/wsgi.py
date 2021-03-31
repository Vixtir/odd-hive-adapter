import os
from logging.config import dictConfig
from config import get_env
from odd_contract import init_flask_app, init_controller
from controllers import OpenDataDiscoveryController
from adapter import HiveAdapter

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
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
)


def create_app(conf):
    app = init_flask_app()
    app.config.from_object(conf)
    hive_adapter = HiveAdapter(
        host_name=get_env("HOST_NAME"),
        port=get_env("PORT"),
        user=get_env("USER"),
        auth=get_env("AUTH"),
        password=get_env("PASSWORD"),
    )

    init_controller(OpenDataDiscoveryController(hive_adapter=hive_adapter))
    return app


application = create_app(
    os.environ.get("FLASK_CONFIG") or "config.DevelopmentConfig"
)
