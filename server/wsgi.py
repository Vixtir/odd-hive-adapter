import os
from logging.config import dictConfig
from odd_contract import init_flask_app, init_controller
from controllers import OpenDataDiscoveryController
from adapter import HiveAdapter
from cache import HiveDataCache
from scheduler import Scheduler
from flask import Response
from flask_compress import Compress

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
    app.add_url_rule('/health', "healthcheck", lambda: Response(status=200))

    Compress().init_app(app)

    hive_data_cache = HiveDataCache()

    init_controller(OpenDataDiscoveryController(hive_data_cache=hive_data_cache,
                                                unit_id=app.config["HIVE_HOST_NAME"]))

    with app.app_context():
        Scheduler(
            hive_adapter=HiveAdapter(
                host_name=app.config["HIVE_HOST_NAME"],
                port=app.config["HIVE_PORT"]),
            hive_data_cache=hive_data_cache
        ).start_scheduler(interval_minutes=int(app.config['SCHEDULER_TIMEOUT_MINUTES']))

    return app


application = create_app(
    os.environ.get("FLASK_CONFIG") or "config.DevelopmentConfig"
)
