#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys, os, shutil, time

import subprocess
import tempfile
import re
import unittest
import redis,json

from gi.repository import GLib, GObject

from common import *
from jobs import getFileType, Transcode, MD5, Filmstrip, FFmpegInfo, Thumbnail
from models import Status, Job, JobCollection, Media
from backbone import deep_get
from monitor import Monitor


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


def compare_images(img1, img2, threshold=0.0, diffout='null:'):
    """
Compares two image files.
Returns a tuple of (match, difference), where 'match' is a boolean and
'difference' the calculated difference index.
Threshold is a float greater than or equal to 0 (0 is exact match).
If 'diffout' is given the difference between images is saved there.
    """

    diff = -1
    match = False
    cmd = ['compare', '-metric', 'RMSE', img1, img2, diffout]

    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)

        d = re.findall('\((.+)\)', output)
        if d:
            diff = float(d[0])
            match = (diff <= threshold)

    finally:
        return (match, diff)


class JobMock(Job):
    def save(self, *args, **kwargs):
        pass


class TestMD5(unittest.TestCase):
    def test_nonexistant(self):
        """"MD5 should emit 'error' if the file can not be opened"""
        job = { 'output': {} }
        md5 = MD5(job, src='golden/does_not_exist.txt')

        smon = SignalMonitor(md5, *_job_signals)
        md5.start()

        self.assertEqual(smon.start_count, 1, 'start is emmited only once')
        self.assertEqual(smon.error_count, 1, 'error is emmited when src can not be opened')
        self.assertEqual(smon.status_count, 0, 'status is not emmited when src can not be opened')
        self.assertEqual(smon.progress_count, 0, 'progress is not emmited when src can not be opened')
        self.assertEqual(smon.success_count, 0, 'success is not emmited when src can not be opened')
        self.assertEqual(smon.finished_count, 0, 'finished is not emmited when src can not be opened')

    def test_golden(self):
        """"MD5 should fill job.output.checksum with the correct value"""
        job = { 'output': {} }
        md5 = MD5(job, src='golden/a3bb96c411fa6c4a28305b87caab9aad.txt')

        smon = SignalMonitor(md5, *_job_signals)
        md5.start()

        self.assertEqual(smon.start_count, 1, 'start is emmited only once')
        self.assertEqual(smon.error_count, 0, 'error is not emmited')
        self.assertTrue(smon.status_count >=1, 'status is emmited at least once')
        self.assertTrue(smon.progress_count >=1, 'progress is emmited at least once')
        self.assertEqual(smon.success_count, 1, 'success is emmited only once')
        self.assertEqual(smon.finished_count, 1, 'finished is emmited only once')
        self.assertEqual(job['output']['checksum'], 'a3bb96c411fa6c4a28305b87caab9aad', 'computed checksum matches')


class TestFFmpegInfo(unittest.TestCase):
    def test_image_input(self):
        """FFmpegInfo should work with image files"""
        job = {
            'output': {
                'metadata': {}
            }
        }

        golden = {
            'output.metadata.audio': {},
            'output.metadata.synched': True,
            'output.metadata.durationraw': '00:00:00.04',
            'output.metadata.video.resolution.h': 960,
            'output.metadata.video.resolution.w': 1280,
            'output.metadata.video.aspectString': '4:3',
        }
        info = FFmpegInfo(job, src='golden/torta.jpeg')
        smon = SignalMonitor(info, *_job_signals)

        info.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 1, 'job finised without errors')
        self.assertEqual(smon.error_count, 0, 'job finised without errors')
        self.assertTrue( compare_dict_to_ref(job, golden), 'output matches expected info')

        duration, found = deep_get(job, 'output.metadata.durationsec')
        self.assertTrue(found, 'has filled metadata.durationsec')
        self.assertAlmostEqual(duration, 0.04, places=2, msg='duration is 1 frame at 25fps (0.04 seconds)')

        aspect, found = deep_get(job, 'output.metadata.video.aspect')
        self.assertTrue(found, 'has filled metadata.video.aspect')
        self.assertAlmostEqual(aspect, 1.333, places=3, msg='aspect is 1.333.. (4:3)')

    def test_video_input(self):
        """FFmpegInfo should work with video files"""
        job = {
            'output': {
                'metadata': {}
            }
        }

        golden = {
            'output.metadata.audio.channels': 'mono',
            'output.metadata.audio.sample_rate': 22050,
            'output.metadata.audio.channels': 'mono',
            'output.metadata.synched': True,
            'output.metadata.durationraw': '00:00:06.37',
            'output.metadata.video.container': 'avi',
            'output.metadata.video.fps': 30.0,
            'output.metadata.video.resolution.h': 240,
            'output.metadata.video.resolution.w': 320,
            'output.metadata.video.aspectString': '4:3',
        }
        info = FFmpegInfo(job, src='golden/clavija_bronce.avi')
        smon = SignalMonitor(info, *_job_signals)

        info.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 1, 'job finised without errors')
        self.assertEqual(smon.error_count, 0, 'job finised without errors')

        self.assertTrue( compare_dict_to_ref(job, golden), 'output matches expected info')

        duration, found = deep_get(job, 'output.metadata.durationsec')
        self.assertTrue(found, 'has filled metadata.durationsec')
        self.assertAlmostEqual(duration, 6.366, places=2, msg='duration is about 6.366 seconds')

        aspect, found = deep_get(job, 'output.metadata.video.aspect')
        self.assertTrue(found, 'has filled metadata.video.aspect')
        self.assertAlmostEqual(aspect, 1.333, places=3, msg='aspect is 1.333.. (4:3)')

    def test_nonexistant(self):
        """FFmpegInfo should error for not existing or not recognized files"""
        job = {
            'output': {
                'metadata': {}
            }
        }

        info = FFmpegInfo(job, src='golden/does_not_exist.txt')
        smon = SignalMonitor(info, *_job_signals)

        info.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 0, 'job finised without errors')
        self.assertEqual(smon.error_count, 1, 'FFmpegInfo emits "error" if file is not found or is not recognized')


class TestThumbnail(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix='.jpg')
        self.tmp = self._tmp.name

    def tearDown(self):
        self._tmp.close()

    def do_thumb(self, src, golden):
        job = {
            'output': {
                'files': [],
            }
        }

        thum = Thumbnail(job, src=src, dst=self.tmp)
        smon = SignalMonitor(thum, *_job_signals)

        thum.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 1, 'job finised without errors')

        match, diff = compare_images(golden, self.tmp)
        self.assertEqual(match, True, 'generated thumbnail matches expected image.')

    def test_image_input(self):
        self.do_thumb(src='golden/torta.jpeg', golden='golden/torta_thumb.jpg')

    def test_video_input(self):
        self.do_thumb(src='golden/clavija_bronce.avi', golden='golden/clavija_thumb.jpg')

    def test_nonexistant(self):
        """Thumbnail should error for not existing or not recognized files"""
        job = {
            'output': {
                'files': [],
            }
        }

        thum = Thumbnail(job, src='golden/does_not_exist.txt', dst=self.tmp)
        smon = SignalMonitor(thum, *_job_signals)

        thum.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 0, 'job finised without errors')
        self.assertEqual(smon.error_count, 1, 'Thumbnail emits "error" if file is not found or is not recognized')


class TestFilmstrip(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix='.mp4')
        self.tmp = self._tmp.name

    def tearDown(self):
        self._tmp.close()

    def do_filmstrip(self, src, golden):
        job = {
            'output': {
                'files': [],
            }
        }

        film = Filmstrip(job, src=src, dst=self.tmp)
        smon = SignalMonitor(film, *_job_signals)

        film.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 1, 'job finised without errors')

        # XXX FIXME: take a single frame and compare?
        # XXX FIXME: compare one by one?
        # match, diff = compare_images(golden, self.tmp)
        # self.assertEqual(match, True, 'generated thumbnail matches expected image.')

    def test_image_input(self):
        self.do_filmstrip(src='golden/torta.jpeg', golden=None)

    def test_video_input(self):
        self.do_filmstrip(src='golden/clavija_bronce.avi', golden=None)

    def test_nonexistant(self):
        """Filmstrip should error for not existing or not recognized files"""
        job = {
            'output': {
                'files': [],
            }
        }

        film = Filmstrip(job, src='golden/does_not_exist.txt', dst=self.tmp)
        smon = SignalMonitor(film, *_job_signals)

        film.start()

        # hate hate hate.
        # FIXME: need to wait for the process to start.
        time.sleep(1)

        ctx = GLib.MainContext.default()
        while ctx.pending():
            ctx.iteration()

        self.assertEqual(smon.finished_count, 0, 'job finised without errors')
        self.assertEqual(smon.error_count, 1, 'Filmstrip emits "error" if file is not found or is not recognized')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main(verbosity=2)
