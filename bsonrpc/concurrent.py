'''
'''

from .options import ThreadingModel


def _spawn_thread(fn, *args, **kwargs):
    from threading import Thread
    t = Thread(target=fn, args=args, kwargs=kwargs)
    t.start()
    return t


def _spawn_greenlet(fn, *args, **kwargs):
    from gevent import Greenlet
    g = Greenlet(fn, *args, **kwargs)
    g.start()
    return g


def spawn(threading_model, fn, *args, **kwargs):
    if threading_model == ThreadingModel.GEVENT:
        return _spawn_greenlet(fn, *args, **kwargs)
    elif threading_model == ThreadingModel.THREADS:
        return _spawn_thread(fn, *args, **kwargs)


def _new_queue(*args, **kwargs):
    from queue import Queue
    return Queue(*args, **kwargs)


def _new_gevent_queue(*args, **kwargs):
    from gevent.queue import Queue
    return Queue(*args, **kwargs)


def new_queue(threading_model, *args, **kwargs):
    if threading_model == ThreadingModel.GEVENT:
        return _new_gevent_queue(*args, **kwargs)
    elif threading_model == ThreadingModel.THREADS:
        return _new_queue(*args, **kwargs)
