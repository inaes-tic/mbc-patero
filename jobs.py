from gi.repository import GLib, GObject

import Process

import tempfile
import hashlib
import os
import re
import math


from Melt import Transcode as MeltTranscode

_file_types = [
    {
    'type':    'image',
    'seconds': 0,
    'pattern': re.compile(r'\.(bmp|gif|jpg|png|yuv|pix|dpx|exr|jpeg|pam|pbm|pcx|pgm|pic|ppm|ptx|sgi|tif|tiff|webp|xbm|xwd)$', re.I),
    },
    {
    'type':    'video',
    'seconds': 5,
    'pattern': re.compile(r'\.(webm|mp4|flv|avi|mpeg|mpeg2|mpg|mov|m4v|mkv|ogm|ogg)$', re.I),
    }
]

def getFileType(filename):
    for _type in _file_types:
        if _type['pattern'].search(filename):
            return _type
    return None

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
        self.emit ('status', 'Calculating checksum')
        self.emit ('progress', 100.0)
        self.alldone()

class Filmstrip(JobBase):
    def __init__(self, job, src=None, dst=None):
        JobBase.__init__(self, job, src, dst)
        self.total_time = None

    def start (self):
        self.emit ('start', self.src, self.dst)
        self.emit ('status', 'Making filmstrip video')
        prog = '-an -r 1 -vf scale=200:ih*200/iw -vcodec libx264'.split()
        head = ['ffmpeg', '-i', self.src]
        head.extend(prog)
        prog = head
        prog.append('-y')
        prog.append(self.dst)
        p = self.spawn(prog)
        p.connect ('exit', self._on_exit)

    def stderr_cb (self, o, s):
        def timetuple_to_seconds(ttuple):
            ttuple = ttuple[:-1]
            total = 0
            for idx,v in enumerate(reversed(ttuple)):
                total += int(v) * (60**idx)
            return total

        # HH:MM:SS.ff
        durations = re.findall(r'Duration: (\d+):(\d+):(\d+).(\d+)', s)
        if durations and not self.total_time:
            # not using frame number for now.
            self.total_time = timetuple_to_seconds(durations[0])

        current = re.findall(r'time=(\d+):(\d+):(\d+).(\d+)', s)
        if current and self.total_time:
            current = timetuple_to_seconds(current[0])
            progress = (100.0*current) / self.total_time
            self.emit('progress', progress)


    def _on_exit(self, process, ret):
        if ret:
            self.emit('error', 'FFmpeg error')

        else:
            self.job['output']['files'].append(self.dst)
            self.alldone()



class FFmpegInfo(JobBase):
    def __init__(self, job, src=None, dst=None):
        JobBase.__init__(self, job, src, dst)
        self.output = []

    def start (self):
        self.emit ('start', self.src, self.dst)
        self.emit ('status', 'Extracting metadata')
        prog = 'ffprobe -show_format -show_streams'.split()
        prog.append(self.src)
        p = self.spawn(prog)
        p.connect ('exit', self._on_exit)

    def stdout_cb (self, o, s):
        self.output.append(s)

    def _on_exit(self, process, ret):
    # helper functions, the real stuff begins a little below this.
        def to_dict(elems):
            ret = {}
            for elem in elems:
                k,v = elem.split('=')
                ret[k] = v
            return ret

        def seconds_to_human(secs):
            msecs, secs= math.modf(secs)
            secs = int(secs)

            m,s = divmod(secs, 60)
            h,m = divmod(m, 60)

            # hate hate hate.
            return ('%i:%.2i:%.2i' % (h,m,s)) + ('%.2f'%msecs)[1:]

        def find_stream_by_codec(streams, codec):
            for s in streams.values():
                if codec == s['codec_type']:
                    return s
            return None

        def extract_audio_info(stream, fmt):
            ret = {}
            if not stream:
                return ret

            ret['codec'] = stream.get('codec_name', '')
            ret['sample_rate'] = int( stream.get('sample_rate', 0) )
            ## XXX: audio bitrate (like 128k mp3) is missing.
            ##ret['bitrate']
            ret['channels'] = {'1':'mono', '2':'stereo'}.get(stream.get('channels', 0), '')
            return ret

        def extract_video_info(stream, fmt):
            ret = {}
            if not (stream and fmt):
                return ret

            ret['container'] = fmt.get('format_name', '').split(',')[0]
            ret['bitrate'] = int( fmt.get('bit_rate', 0) )
            ret['codec'] = stream.get('codec_name', '')
            num,den = [float(x) for x in stream.get('r_frame_rate', '0.0/1').split('/')]
            if den:
                ret['fps'] = num/den
            else:
                ret['fps'] = 0.0

            ret['resolution'] = res = {'w': 0, 'h': 0}
            res['w'] = int( stream.get('width', 0) )
            res['h'] = int( stream.get('height', 0) )

            # save aspect ratio for auto-padding
            aspect = stream.get('display_aspect_ratio', '')
            if aspect:
                n,d = [float(x) for x in aspect.split(':')]
                ret['aspect'] = n / d
                ret['aspectString'] = aspect
            else:
                w,h = res['w'], res['h']
                if w:
                    ret['aspect'] = float(w) / h
                    ret['aspectString'] = '%i:%i' % (w,h)
                else:
                    ret['aspect'] = 0.0
                    ret['aspectString'] = ''

            # save pixel ratio for output size calculation
            aspect = stream.get('sample_aspect_ratio', '1:1')
            n,d = [float(x) for x in aspect.split(':')]
            ret['pixel'] = pixel = n / d
            ret['pixelString'] = aspect

            # correct video resolution when pixel aspectratio is not 1
            ret['resolutionSquare'] = res = {'w': 0, 'h': 0}
            res['w'] = int( stream.get('width', 0) )
            res['h'] = int( stream.get('height', 0) )
            if pixel == 1 or pixel == 0:
                res['w'] = res['w'] * pixel

            #rotate is missing.
            return ret

        # here.
        if ret:
            self.emit('error', 'ffprobe error')

        else:
            output = ''.join(self.output)
            streams_raw = re.findall(re.compile('\[STREAM\](.+?)\[/STREAM\]', re.M | re.S), output)
            fmt_raw = re.findall(re.compile('\[FORMAT\](.+?)\[/FORMAT\]', re.M | re.S), output)

            streams_raw = [item.strip().split('\n') for item in streams_raw]
            fmt_raw = [item.strip().split('\n') for item in fmt_raw]

            streams = {}
            for idx, s in enumerate(streams_raw):
                streams[idx] = to_dict(s)
            fmt = to_dict(fmt_raw[0])

# XXX:  faltan title , que puede estar en stream de video o en format
# XXX:  date y artist
            meta = {}
            meta['synched'] = fmt.get('start_time', None) == '0.000000'
            meta['durationsec'] = d = float( fmt.get('duration', 0 ))
            meta['durationraw'] = seconds_to_human(d)
            meta['audio'] = extract_audio_info(find_stream_by_codec(streams, 'audio'), fmt)
            meta['video'] = extract_video_info(find_stream_by_codec(streams, 'video'), fmt)

            self.job['output']['metadata'].update(meta)

            self.alldone()

class Transcode(JobBase):
    def __init__(self, job, src=None, dst=None):
        JobBase.__init__(self, job, src, dst)

        m = self.melt = MeltTranscode(src=src, dst=dst)

        m.connect('status', self._emit_msg, 'status')
        m.connect('progress', self._emit_msg, 'progress')
        m.connect('error', self._emit_msg, 'error')
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


