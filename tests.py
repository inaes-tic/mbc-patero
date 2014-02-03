#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys, os, shutil, time
import unittest
import redis,json

_job_signals = ['start', 'finished', 'status', 'error', 'success', 'progress', 'start-audio', 'start-video']

# shamelessly copied from pitivi.
class SignalMonitor(object):
    def __init__(self, obj, *signals):
        self.signals = signals
        self.connectToObj(obj)

    def connectToObj(self, obj):
        self.obj = obj
        for signal in self.signals:
            obj.connect(signal, self._signalCb, signal)
            setattr(self, self._getSignalCounterName(signal), 0)
            setattr(self, self._getSignalCollectName(signal), [])

    def disconnectFromObj(self, obj):
        obj.disconnect_by_func(self._signalCb)
        del self.obj

    def _getSignalCounterName(self, signal):
        field = '%s_count' % signal.replace('-', '_')
        return field

    def _getSignalCollectName(self, signal):
        field = '%s_collect' % signal.replace('-', '_')
        return field

    def _signalCb(self, obj, *args):
        name = args[-1]
        field = self._getSignalCounterName(name)
        setattr(self, field, getattr(self, field, 0) + 1)
        field = self._getSignalCollectName(name)
        setattr(self, field, getattr(self, field, []) + [args[:-1]])


# just because everybody has to improve global warming.
def compare_dict_to_ref(d, ref, debug=False, failEarly=True):
    for k,v in ref.iteritems():
        value, found = deep_get(d, k)
        if not found:
            if debug: logging.debug('Key not found: %s', k)
            if failEarly: return False
        if value != v:
            if debug: logging.debug('Key %s, different values. Expected: %s Got: %s' %(k, unicode(v), unicode(value)))
            if failEarly: return False
    return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main(verbosity=2)
