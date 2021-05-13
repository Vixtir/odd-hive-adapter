import logging
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from adapter import HiveAdapter
from cache import HiveDataCache


class Scheduler:
    def __init__(self, hive_adapter: HiveAdapter, hive_data_cache: HiveDataCache) -> None:
        self.__hive_adapter = hive_adapter
        self.__scheduler = BackgroundScheduler(executors={'default': ThreadPoolExecutor(1)})
        self.__hive_data_cache = hive_data_cache

    def start_scheduler(self, interval_minutes: int):
        self.__scheduler.start()
        logging.info(f"Scheduler started")
        self.__scheduler.add_job(self.__retrieve_data_entities_data,
                                 trigger='interval',
                                 minutes=interval_minutes,
                                 next_run_time=datetime.utcnow())

    def __retrieve_data_entities_data(self):
        self.__hive_data_cache \
            .cache_data_entities(self.__hive_adapter.get_datasets())
