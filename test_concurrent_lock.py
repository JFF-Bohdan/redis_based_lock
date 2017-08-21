import datetime
import logging
import random
import sys
import threading
import time
import uuid

import redis


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_PASSWORD = None
REDIS_DATABASE = 0

WORKERS_COUNT = 10

WORK_MIN_SECS = 4
WORK_MAX_SECS = 8
EXPERIMENT_TIME_SECS = 30

MAX_LOCK_TIME_WITHOUT_CONFIRMATION = 6

ATOMIC_LOCK_TIME_MIN_SEC = 1.5
ATOMIC_LOCK_TIME_MAX_SEC = 4.2


def main():
    target_uid = str(uuid.uuid4())

    logger = test_server_init_logs()
    logger.info("app started, target_uid = {}".format(target_uid))

    threads = []
    for i in range(WORKERS_COUNT):
        thread_name = "thread-{}".format(i)
        thread = WorkerThread(thread_name, logger, target_uid)
        thread.setDaemon(True)
        threads.append(thread)

    for thread in threads:
        thread.start()

    logger.info("starting experiment for {} seconds".format(EXPERIMENT_TIME_SECS))
    time.sleep(EXPERIMENT_TIME_SECS)

    for thread in threads:
        thread.stop()

    for thread in threads:
        thread.join()

    logger.info("app finished")


def test_server_init_logs():
    logger = logging.getLogger("main")

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)
    return logger


class WorkerThread(threading.Thread):
    def __init__(self, name, logger, target_uid):
        super().__init__(name=name)
        self._stop_event = threading.Event()
        self.logger = logger
        self._redis_connection = None
        self._target_uid = target_uid

    def run(self):
        self.logger.info("[{}] - active".format(self.name))

        self._init_redis_connection()

        while not self._stop_event.is_set():
            max_work_time = self._get_lock_time(WORK_MIN_SECS, WORK_MAX_SECS)
            log_lock_start_timestamp = datetime.datetime.now().isoformat()
            is_locked = self._lock_target(MAX_LOCK_TIME_WITHOUT_CONFIRMATION, log_lock_start_timestamp)

            if is_locked:
                self.logger.warn("[{}] - locks target for {} seconds".format(self.name, max_work_time))
                self._work(max_work_time)

                # we need this gap to let other threads pick target
                time.sleep(self._get_gap_time())
                continue

            time.sleep(0.05)

        self.logger.info("[thread-{}] - stopped".format(self.name))

    def _init_redis_connection(self):
        self._redis_connection = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
        )

    def _work(self, max_work_time):
        tm_begin = time.time()

        def_atomic_sleep = random.uniform(ATOMIC_LOCK_TIME_MIN_SEC, ATOMIC_LOCK_TIME_MAX_SEC)

        while (time.time() - tm_begin) < max_work_time:
            spent_time = (time.time() - tm_begin)
            atomic_sleep = min(max_work_time - spent_time, def_atomic_sleep)
            atomic_sleep = round(atomic_sleep, 1)
            if atomic_sleep == 0:
                break

            self.logger.warn("[{}] - atomic sleep for {}".format(self.name, atomic_sleep))
            time.sleep(atomic_sleep)
            self._update_expiration_time()

        self._release_lock()

    @staticmethod
    def _get_lock_time(wait_min, wait_max):
        return random.randint(wait_min, wait_max)

    def _update_expiration_time(self):
        self._redis_connection.expire(self._get_lock_name(), MAX_LOCK_TIME_WITHOUT_CONFIRMATION)

    @staticmethod
    def _get_gap_time():
        return random.uniform(0.05, 0.25)

    def stop(self):
        self._stop_event.set()

    def _lock_target(self, lock_time, lock_value):
        return self._redis_connection.set(self._get_lock_name(), lock_value, ex=lock_time, nx=True)

    def _get_lock_name(self):
        return "target_{}".format(self._target_uid)

    def _release_lock(self):
        self._redis_connection.delete(self._get_lock_name())


if __name__ == "__main__":
    main()
