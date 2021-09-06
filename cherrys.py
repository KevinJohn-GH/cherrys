"""
Cherrys is a Redis backend for CherryPy sessions.

The redis server must support SETEX http://redis.io/commands/setex.

Relies on redis-py https://github.com/andymccurdy/redis-py.
"""

import threading
import logging
import sys

try:
    import cPickle as pickle
except ImportError:
    import pickle

from cherrypy.lib.sessions import Session
from concurrent.futures import ThreadPoolExecutor, wait
import redis

log = logging.getLogger(__name__)


def _wait_first_succeed(futures, fn):
    ret = None
    while len(futures) > 0:
        done, not_done = wait(futures, return_when='FIRST_COMPLETED')
        for done_future in done:
            exception = done_future.exception()
            if exception:
                if type(exception) == AssertionError:
                    log.debug('[{fn}] Future Exception: {exception}'.format(fn=fn, exception=exception))
                else:
                    log.error('[{fn}] Future Exception: {exception}'.format(fn=fn, exception=exception))
                continue
            else:
                ret = done_future.result()
                return ret
        futures = list(not_done)
    return ret


class RedisSession(Session):

    @classmethod
    def setup(cls, **kwargs):
        """Set up the storage system for redis-based sessions.

        Called once when the built-in tool calls sessions.init.
        """
        # overwritting default settings with the config dictionary values
        for k, v in kwargs.items():
            setattr(cls, k, v)

        redis_instances = cls.redis_instances
        cls.caches = []

        for redis_instance in redis_instances:
            cache = redis.StrictRedis(
                host=redis_instance['host'],
                port=redis_instance['port'],  # cherrys in charge of converting str to int
                db=redis_instance['db'],
                password=redis_instance.get('password', None),
                socket_timeout=redis_instance.get('socket_timeout', 10),
                socket_connect_timeout=redis_instance.get('socket_connect_timeout', 1))
            cls.caches.append(cache)

    def _exists_worker(self, cache):
        result = bool(cache.exists(self.id))
        if result:
            return result
        else:
            kwargs = cache.connection_pool.connection_kwargs
            raise AssertionError('Key {id} not exist in redis://{host}:{port}/{db}'.format(
                id=self.id, host=kwargs['host'], port=kwargs['port'], db=kwargs['db']))

    def _exists(self):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._exists_worker, cache=cache) for cache in self.caches]
        result = _wait_first_succeed(futures=futures, fn=sys._getframe().f_code.co_name)
        return bool(result)

    def _load_worker(self, cache):
        result = cache.get(self.id)
        if not result:
            raise AssertionError('Get key {id} return {result}'.format(id=self.id, result=result))
        return pickle.loads(result)

    def _load(self):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._load_worker, cache=cache) for cache in self.caches]
        result = _wait_first_succeed(futures=futures, fn=sys._getframe().f_code.co_name)
        return result

    def _save_worker(self, cache, expiration_time):

        pickled_data = pickle.dumps(
            (self._data, expiration_time),
            pickle.HIGHEST_PROTOCOL)

        result = cache.setex(self.id, pickled_data, self.timeout * 60)

        if not result:
            kwargs = cache.connection_pool.connection_kwargs
            raise AssertionError('Session data for id {id} not set into redis://{host}:{port}/{db}.'.format(
                id=self.id, host=kwargs['host'], port=kwargs['port'], db=kwargs['db']))

    def _save(self, expiration_time):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._save_worker, cache=cache, expiration_time=expiration_time)
                   for cache in self.caches]
        _wait_first_succeed(futures=futures, fn=sys._getframe().f_code.co_name)

    def _delete_worker(self, cache):
        cache.delete(self.id)

    def _delete(self):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._delete_worker, cache=cache) for cache in self.caches]

    # http://docs.cherrypy.org/dev/refman/lib/sessions.html?highlight=session#locking-sessions
    # session id locks as done in RamSession

    locks = {}

    def acquire_lock(self):
        """Acquire an exclusive lock on the currently-loaded session data."""
        self.locked = True
        self.locks.setdefault(self.id, threading.RLock()).acquire()

    def release_lock(self):
        """Release the lock on the currently-loaded session data."""
        self.locks[self.id].release()
        self.locked = False
