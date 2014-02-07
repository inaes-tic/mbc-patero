#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import uuid
from multiprocessing import Process, Queue
from Queue import Empty, Full

from gi.repository import GLib, GObject


def worker_fn(pubsub, queue, client_id):
    g = pubsub.listen()
    while True:
        try:
            msg = g.next()
            queue.put(msg)
        except StopIteration:
            logging.error('Redis: got StopIteration')

class RedisListener(GObject.GObject):
    """
RedisListener:
Uses the blocking pubsub.listen() on another process, when a message arrives
it is sent to the parent and a 'message' signal is emitted.

The 'redis' parameter is a connection like the one from  calling redis.Redis()
    """
    __gsignals__ = {
        'message': (GObject.SIGNAL_RUN_FIRST, None, [GObject.TYPE_PYOBJECT]),
    }

    def __init__(self, redis, client_id=None):
        GObject.GObject.__init__(self)
        if client_id is None:
            self.client_id = unicode( uuid.uuid4() )
        else:
            self.client_id = client_id

        pubsub = redis.pubsub()
        pubsub.subscribe('__RedisListener_'+unicode(self.client_id))

        self.pubsub = pubsub
        self.queue = Queue()

        GLib.timeout_add(50, self._check_queue)

        self.worker = Process(target=worker_fn, args=[self.pubsub, self.queue, self.client_id])
        self.worker.start()

    def _check_queue(self):
        while not self.queue.empty():
            try:
                msg = self.queue.get_nowait()
                if msg['type'] == 'message':
                    self.emit('message', msg)
            except Empty:
                pass
        return True

    def subscribe(self, channel):
        self.pubsub.subscribe(channel)

    def unsusbcribe(self, channel):
        self.pubsub.unsubscribe(channel)


if __name__ == '__main__':
    import redis
    redis = redis.Redis()

    def on_message_cb(listener, message):
        print 'RedisListener got message: ', message

    listener = RedisListener(redis=redis, client_id='1234')
    listener.subscribe('RedisListener.demo')
    listener.connect('message', on_message_cb)

    loop = GLib.MainLoop()
    loop.run()
