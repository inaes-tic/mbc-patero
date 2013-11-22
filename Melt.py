from gi.repository import GLib, GObject

import Process

import tempfile
import os
import re

class Transcode(GObject.GObject):
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

    def __init__(self, src, dst=None, destdir=None):
        GObject.GObject.__init__(self)

        self.errstr = ''
        self.fail = False

        #XXX: this breaks Zumo.
        #src = GLib.filename_from_uri (src)[0]

        if (dst == None):
            if (destdir):
                dst = destdir + '/' + src.split('/')[-1].strip() + '.m4v'
            else:
                dst = src.strip() + '.m4v'

        self.src = src
        self.dst = dst
        (fd, self.mlt) = tempfile.mkstemp('.mlt')
        os.close(fd)

    def start (self):
        self.emit ('start', self.src, self.dst)
        self.do_pass1()

    def spawn (self, p):
        p = Process.Handler (p)
        p.connect ('stderr', self.stderr_cb)
        return p

    def stderr_cb (self, o, s):
        print 'stderr', s
        if re.findall (r'Failed to load', s):
            self.emit ('error', s)
            self.fail = True

        perc = 0
        try:
            perc = float(re.findall(r'percentage:\s+(\d+).$', s)[0])
        except:
            return True

        if (perc):
            self.emit('progress', perc)
        else:
            self.errstr += (s)

        return True

    def check_fail (self, nxt):
        if not self.fail:
            nxt()
        else:
            self.emit('finished', self.src, self.dst)

    def do_pass1 (self):
        prog = ['melt','-progress', self.src.strip(),
                '-filter', 'sox:analysis',
                '-consumer', 'xml:' + self.mlt.strip(),
                'video_off=1', 'all=1']

        self.emit('status', 'audio-norm')
        self.emit('start-audio')
        p = self.spawn(prog)
        p.connect ('exit', lambda o, r: self.check_fail(self.do_pass2))

    def do_pass2 (self):
        prog = ['melt','-progress', self.mlt.strip(),
                '-consumer', 'avformat:' + self.dst.strip(),
                'properties=H.264', 'strict=experimental', 'progressive=1']

        self.emit('status', 'video-transcode')
        self.emit('start-video')
        p = self.spawn(prog)
        p.connect ('exit', lambda o, r: self.check_fail(self.alldone))

    def alldone (self):
        dst = self.dst
        if self.mlt:
            #os.remove (self.mlt)
            self.mlt = None

        self.emit('success', self.dst)
        self.emit('finished', self.src, self.dst)

def dump(a=None, b=None, c=None, d=None):
    print "DUMP:", b, c, d

if __name__ == "__main__":
    import os

    t = []

    for i in range(10):
        e = Transcode('file://' + os.path.realpath('test.mp4'),
                      os.path.realpath('test' + str(i) + '.m4v'))
        t.append(e)

        I = str(i)
        e.connect ('start', dump, 'start' + I)
        e.connect ('finished', dump, 'finished' + I)
        e.connect ('status', dump, 'status' + I)
        e.connect ('error', dump, 'error' + I )

    for i in range (9):
        print i, '->', i+1
        t[i].connect ('finished', lambda x, y, z: t[i+1].start())

    t[0].start()
    l = GLib.MainLoop()
    l.run()
