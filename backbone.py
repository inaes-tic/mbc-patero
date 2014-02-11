# -*- coding: utf-8 -*-

import re
import uuid
import redis, json
from copy import deepcopy
import threading

import pymongo
from pymongo import MongoClient

from common import *
from gredis import RedisListener
from multiprocessing import Process, Queue, Event
from Queue import Empty, Full

redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

_client_id = unicode( uuid.uuid4() )

class BackboneRedisListener(RedisListener):
    def __init__(self, redis, client_id=None):
        self.token_queue = Queue()
        self.token_map = {}
        self.tokenlck = threading.Lock()

        super(BackboneRedisListener, self).__init__(redis, client_id)

    def create_worker(self):
        kwargs = {
            'pubsub':    self.pubsub,
            'queue':     self.queue,
            'client_id': self.client_id,
            'token_queue': self.token_queue,
        }
        self.worker = Process(target=self.worker_fn, kwargs=kwargs)
        self.worker.start()

        self.tokenworker = threading.Thread(target=self.token_worker_fn)
        self.tokenworker.daemon = True
        self.tokenworker.start()

    def watchToken(self, token):
        ev = threading.Event()
        with self.tokenlck:
            self.token_map[token] = ev
        return ev

    def worker_fn(self, pubsub=None, queue=None, client_id=None, *args, **kwargs):
        token_queue = kwargs['token_queue']
        g = pubsub.listen()
        while True:
            try:
                message = g.next()
                queue.put(message)

                if message['type'] == 'message':
                    data = json.loads(message['data'])
                    token = data.get('token', None)
                    if token is not None:
                        token_queue.put(token)
            except StopIteration:
                logging.error('Redis: got StopIteration')

    def token_worker_fn(self, *args, **kwargs):
        fulfilled = []
        token_queue = self.token_queue
        token_map = self.token_map

        def process_tokens():
            to_remove = []

            fulfilled.append( token_queue.get() )
            while not token_queue.empty():
                try:
                    fulfilled.append( token_queue.get_nowait() )
                except Empty:
                    pass

            with self.tokenlck:
                for token in fulfilled:
                    ev = token_map.pop(token, None)
                    to_remove.append(token)
                    if ev is None:
                        continue
                    else:
                        ev.set()

                for token, ev in token_map.iteritems():
                    if ev.is_set():
                        to_remove.append(token)
                        token_map.pop(token)

            for token in to_remove:
                fulfilled.remove(token)

        while True:
            process_tokens()


listener = BackboneRedisListener(redis=redis, client_id = _client_id)

# XXX: according to mongo docs we only need to create one client and share it everywhere
client = MongoClient(mongocnstr)
db = client[dbname]

def deep_get(d, key):
    """Given a nested dictionary structure and a path like 'a.b.c' tries
to retrieve it. Returns a tuple of (value, found). If the path is not on
the structure the returned value is also None"""
    found = True
    current = d
    for k in key.split('.'):
        if k in current:
            # current can also be something like "aaaaakbbbb"
            # so while the 'in' test succeeds you can not access it
            # like a dict.
            try:
                current = current[k]
            except:
                found = False
                break
        else:
            found = False
            break

    if not found:
        return (None, False)
    else:
        return (current, True)

class BackboneException(Exception):
    pass

class Base(object):
    """Common stuff for Backbone compatibility.
It is quite tied for now to our redis+mongo structure"""
    # some of this will go away once I get sane 'read' support over Redis.
    colname     = ''
    backend     = ''

    def __init__(self):
        if self.colname:
            self._col = db[self.colname]
        else:
            self._col = None

        self._channel = '_RedisSync.' + re.sub('backend$', '', self.backend)

    def sync(self, method, model=None, options=None, extra={}):
        # XXX: dumb and not generic. for now it only broadcast to Redis.
        if method not in 'create read update delete'.split():
            raise BackboneException('sync(): unknown method %s' % method)

        if model is not None:
            #XXX: need to consider when we pass a collection or an array of dicts.
            if isinstance(model, Model):
                model = model.attributes
            else:
                pass
        else:
            model = self.attributes

        req = {
            'method': method,
            'model': model,
            '_redis_source': _client_id,
        }

        req.update(extra)
        redis.publish(self._channel, json.dumps(req))

class Model(Base):
    """A class that somehow mimics Backbone.Model

Besides the standard Backbone methods (get, set, update) you can also access it
like a normal python dict, however it will not trigger change notifications.

To define your models do someting like:

class MyModel(Model):
    colname = 'the mongo collection' #
    backend = 'backend name' #used for live sync over Redis.
    defaults = {} # optional dictionary with default values.
"""
    idAttribute = '_id'
    id          = None
    defaults    = {}

    def __init__(self, attributes=None):
        super(Model, self).__init__()
        self.collection = None
        self.attributes = deepcopy(self.defaults)
        self._redis_handler = None

        if attributes is not None:
            self.attributes.update(attributes)
            self.id = attributes.get(self.idAttribute, None)


    def bindRedis(self):
        listener.subscribe(self._channel)
        self._redis_handler = listener.connect('message', self._on_backend)

    def _on_backend(self, listener, message):
        data = json.loads(message['data'])
        if message.get('channel', None) != self._channel:
            return

        if data.get('_redis_source', _client_id) == _client_id:
            return

        method = data.get('method', None)
        model = data.get('model', None)
        if model is None:
            return

        if method not in 'create read update delete'.split():
            return

        if model.get(self.idAttribute, None) != self.attributes[self.idAttribute]:
            return

        if method == 'update':
            self.set(model)
        elif method == 'delete':
            self.destroy()

    def __getitem__(self, key):
        return self.attributes[key]

    def __setitem__(self, key, value):
        self.attributes[key] = value

    def __delitem__(self, key):
        del( self.attributes[key] )

    def __iter__(self, *args, **kwargs):
        return iter(self.attributes)

    def __contains__(self, item):
        return item in self.attributes

    def keys(self):
        return self.attributes.keys()

    def values(self):
        return self.attributes.values()

    def items(self):
        return self.attributes.items()

    def iterkeys(self):
        return self.attributes.iterkeys()

    def itervalues(self):
        return self.attributes.itervalues()

    def iteritems(self):
        return self.attributes.iteritems()

    def get(self, *args, **kwargs):
        return self.attributes.get(*args, **kwargs)

    def pop(self, *args, **kwargs):
        return self.attributes.pop(*args, **kwargs)

    def popitem(self, *args, **kwargs):
        return self.attributes.popitem(*args, **kwargs)

    def setdefault(self, *args, **kwargs):
        return self.attributes.setdefault(*args, **kwargs)

    def update(self, *args, **kwargs):
        return self.attributes.update(*args, **kwargs)

    def set(self, attributes, options=None):
        self.attributes = attributes.copy()
        self.id = self.attributes.get(self.idAttribute, None)

    def fetch(self, options=None):
#XXX: need to move all of this into a generic sync.
        if self._col is None:
            return

        # need to keep old attributes and fire a change or something.
        _id = self.id
        if _id is not None:
            ret = self._col.find_one({'_id': _id})
        else:
            ret = self._col.find_one()

        self.attributes = deepcopy(self.defaults)
        if ret is not None:
            self.attributes.update(ret)

    def save(self, attributes=None, options=None):
        if attributes is not None:
            self.attributes.update(attributes)

        method = 'update'
        if self.id is None:
            # ObjectID gives a lot of trouble when(if) we want to send it over Redis.
            self.id = self.attributes.get(self.idAttribute, None) or unicode( uuid.uuid4() )
            self.attributes[self.idAttribute] = self.id
            method = 'create'

        if self._col:
            self._col.update({'_id': self.id}, self.attributes, True)
        self.sync(method)

    def destroy(self, options=None):
        if self.id is not None:
            if self._col:
                self._col.remove({'_id': self.id})
            self.sync('delete')

        if self.collection:
            self.collection.remove(self)

        if self._redis_handler is not None:
            listener.disconnect(self._redis_handler)
            self._redis_handler = None


class Collection(Base):
    """A class that somehow mimics Backbone.Collection
Besides the standard Backbone methods (get, set, update, find, findWhere)
you can also access it like a normal python dict, however it will not trigger
change notifications.

To define your collections do someting like:

class MyCollection(Collection):
    colname = 'the mongo collection' #
    backend = 'backend name' # used for live sync over Redis.
    model   = MyModel
"""

    model = Model

    def __init__(self, models=None, options=None):
        super(Collection, self).__init__()

        self.models = []
        self._models = {}
        self._redis_handler = None

        if models is not None:
            for m in models:
                if isinstance(m, Model):
                    M = m
                else:
                    M = self.model(m)

                self.models.append(M)
                self._models[M.id] = M
                M.collection = self

    def bindRedis(self):
        listener.subscribe(self._channel)
        self._redis_handler = listener.connect('message', self._on_backend)

    def _on_backend(self, listener, message):
        data = json.loads(message['data'])
        if message.get('channel', None) != self._channel:
            return

        if data.get('_redis_source', _client_id) == _client_id:
            return

        method = data.get('method', None)
        model = data.get('model', None)
        if model is None:
            return

        if method not in 'create read update delete'.split():
            return

        mid = model.get(self.model.idAttribute, None)
        mod = self._models.get(mid, None)

        if method == 'update' and mod is not None:
            mod.set(model)
        if method == 'delete' and mod is not None:
            mod.destroy()

        if method == 'create':
            self.add(model)

    def __iter__(self, *args, **kwargs):
        return iter( self.models )

    def __contains__(self, item):
        return item in self.models

    def __len__(self):
        return len(self.models)

    def __getitem__(self, key):
        return self._models[key]

    def get(self, _id):
        return self._models[_id]

    def where(self, attributes, _all=True):
#XXX: probably it would be better to retunr a generator here but then there is
# the case when we modify the stuff we are iterating over.
        def cmp(m):
            for k,v in attributes.iteritems():
                if k not in m:
                    return False
                if m[k] != v:
                    return False
            return True

        if _all:
            return [ m for m in self if cmp(m) ]
        else:
            for m in self:
                if cmp(m):
                    return m

    def findWhere(self, attributes):
        return self.where(attributes, False)

#XXX:
    def fetch(self, options=None):
        # XXX: need to keep old attributes and fire a change or something.
        # XXX: need to implement the merging behaviour inside set(). For now we just reset.

        self.models = []
        self._models = {}

        for m in self._col.find():
            M = self.model(m)
            self.models.append(M)
            self._models[M.id] = M
            M.collection = self


    def add(self, m, options=None):
        if isinstance(m, Model):
            M = m
        else:
            M = self.model(m)

        if M in self.models:
            return

        self.models.append(M)
        self._models[M.id] = M
        M.collection = self

    def remove(self, model, options=None):
        def _remove(m):
            if m not in self:
                return
            self.models.remove(m)
            del( self._models[m.id] )

        if isinstance(model, Model):
            _remove(model)
        else:
            for m in models:
                _remove(m)

#XXX:
    def create(self, attributes, options=None):
        if not isinstance(attributes, Model):
            model = self.model(attributes)
        else:
            model = attributes

        model.save()
        self.add(model)
