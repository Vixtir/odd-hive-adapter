import os
from logging.config import dictConfig
from odd_contract import init_flask_app, init_controller
from server.controllers import OpenDataDiscoveryAdapterController

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})


def create_app(conf):
    app = init_flask_app()
    app.config.from_object(conf)
    init_controller(OpenDataDiscoveryAdapterController())
    return app


application = create_app(os.environ.get("FLASK_CONFIG") or "server.config.DevelopmentConfig")
