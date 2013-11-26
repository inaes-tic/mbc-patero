from gi.repository import GLib, GObject

import Process

import tempfile
import hashlib
import os
import re

from Melt import Transcode as MeltTranscode


class JobBase(GObject.GObject):
    __gsignals__ = {
        'start': (GObject.SIGNAL_RUN_FIRST, None, (str, str)),
        'finished': (GObject.SIGNAL_RUN_FIRST, None, (str,str)),
        'status': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
        'error': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
        'success': (GObject.SIGNAL_RUN_FIRST, None, (str,)),
        'progress': (GObject.SIGNAL_RUN_FIRST, None, (float,)),
        'start-audio': (GObject.SIGNAL_RUN_FIRST, None, ()),
        'start-video': (GObject.SIGNAL_RUN_FIRST, None, ())
    }

    def __init__(self, job, src=None, dst=None):
        GObject.GObject.__init__(self)

        self.errstr = ''
        self.fail = False

        self.job = job
        self.src = src
        self.dst = dst

    def start (self):
        self.emit ('start', self.src, self.dst)
        self.emit ('status', 'Default status message')
        self.alldone()

    def spawn (self, p):
        p = Process.Handler (p)
        p.connect ('stderr', self.stderr_cb)
        p.connect ('stdout', self.stdout_cb)
        return p

    def stderr_cb (self, o, s):
        return True

    def stdout_cb (self, o, s):
        return True

    def check_fail (self, nxt=None):
        if not self.fail:
            if nxt:
                nxt()
        else:
            self.emit('finished', self.src, self.dst)

    def alldone (self):
        self.emit('success', self.dst)
        self.emit('finished', self.src, self.dst)


class MD5(JobBase):
    def __init__(self, job, src=None, dst=None):
        JobBase.__init__(self, job, src, dst)

    def start(self):
        # http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
        self.emit ('start', self.src, self.dst)
        md5 = hashlib.md5()
        with open(self.src,'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                 md5.update(chunk)
        self.job['output']['checksum'] = md5.hexdigest()
        self.emit ('status', 'calculating-checksum')
        self.emit ('progress', 100.0)
        self.alldone()

class Filmstrip(JobBase):
    def __init__(self, job, src=None, dst=None):
        JobBase.__init__(self, job, src, dst)

    def start (self):
        self.emit ('start', self.src, self.dst)
        self.emit ('status', 'Making Filmstrip video')
        prog = '-an -r 1 -vf scale=200:ih*200/iw -vcodec copy'.split()
        head = ['ffmpeg', '-i', self.src]
        head.extend(prog)
        prog = head
        prog.append('-y')
        prog.append(self.dst)
        p = self.spawn(prog)
        p.connect ('exit', self._on_exit)

    def _on_exit(self, process, ret):
        if ret:
            self.emit('error', 'FFmpeg error')

        else:
            self.job['output']['files'].append(self.dst)
            self.alldone()

class FFmpegInfo(JobBase):
    #not now, let Caspa handle this.
    pass

class Transcode(JobBase):
    def __init__(self, job, src=None, dst=None):
        JobBase.__init__(self, job, src, dst)

        m = self.melt = MeltTranscode(src=src, dst=dst)

        m.connect('status', self._emit_msg, 'status')
        m.connect('progress', self._emit_msg, 'progress')
        m.connect('finished', self._finish_cb)
        m.connect('success', self._success_cb)
        m.connect('start', self._start_cb)

    def start(self):
        self.melt.start()

    def _emit_msg(self, melt, payload, msg):
        self.emit(msg, payload)

    def _start_cb(self, melt, src, dst):
        self.emit('start', src, dst)

    def _finish_cb(self, melt, src, dst):
        self.job['output']['files'].append(dst)
        self.emit('finished', src, dst)

    def _success_cb(self, melt, dst):
        self.emit('success', dst)


