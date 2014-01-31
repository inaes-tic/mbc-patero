#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys, os, shutil
import uuid
from collections import deque

from gi.repository import GLib, GObject

import redis,json

from common import *
from jobs import getFileType, Transcode, MD5, Filmstrip, FFmpegInfo, Thumbnail
from models import Status, Job, JobCollection, Media
from monitor import Monitor

class Patero(GObject.GObject):
    def __init__(self):
        GObject.GObject.__init__(self)

        queue = self.queue = JobCollection()
        queue.fetch()

        self.tasks = deque()
        self.status = Status()
        self.running = False

        for job in queue.where({'stage': 'processing'}):
            job.update({
                'stage': 'queued',
                'tasks': [],
                'progress': 0,
            })
            job['output']['files'] = []
            job.save()

        for job in queue.where({'stage': 'moving'}):
            job.destroy()
            self.queue_file(job['input']['path'])

        GLib.timeout_add(500, self.transcode)
        GLib.timeout_add(500, self.send_status)


    def send_status(self, status=None):
        self.status.save({'_id':1, 'running': True})
        return True

    def transcode(self):
        if self.running:
            return True

        job = self.queue.findWhere({'stage': 'queued'})

        if not job:
            return True

        job['stage'] = 'about-to-process'
        job.save()

        def progress_cb(task, progress):
            job = task.job
            ##logging.debug('Progress: %s', progress)
            job['progress'] = progress
            job.save()


        def start_cb(task, src, dst):
            job = task.job
            logging.debug('Start: %s', src)
            job['stage'] = 'processing'
            job['progress'] = 0
            job.save()

        def error_cb(task, msg):
            job = task.job
            # XXX: get rid of all files here?
            logging.error('Error: %s', msg)
            job['stage'] = 'processing-error'
            if job['tasks']:
                job['tasks'][-1]['status'] = 'failed'
                job['tasks'][-1]['message'] = 'Error: ' + msg
            job.save()
            self.tasks.clear()
            self.running = False

        def status_cb(task, msg):
            job = task.job
            logging.debug('Stage: %s', msg)
            if job['tasks']:
                job['tasks'][-1]['status'] = 'done'
            job['tasks'].append({'name':msg, 'status':'processing', 'message':''})
            job.save()

        def start(*args):
            self.running = True
            task = self.tasks.popleft()
            job = task.job
            if job['tasks']:
                job['tasks'][-1]['status'] = 'done'
            task.start()

        def success_cb(task, dst):
            job = task.job

            if job['tasks']:
                job['tasks'][-1]['status'] = 'done'
                job.save()

            if not self.tasks:
                logging.debug('Ok: %s', dst)
                job['stage'] = 'processing-done'
                job['progress'] = 0

                tmp = []
                for filename in job['output']['files']:
                    copy_or_link(filename, output_dir)
                    os.unlink(filename)
                    tmp.append(os.path.join(output_dir, os.path.basename(filename)))
                job['output']['files'] = tmp
                job.save()

                # here tell Caspa the file is ready
                m = Media()
                for key in ['metadata', 'stat']:
                    m.update(job['output'][key])

                m['_id'] = job['output']['checksum']
                m['checksum'] = job['output']['checksum']
                m['files'] = job['output']['files']
                m['file'] = job['output']['transcoded']

                m.save()

                self.running = False
                return
            else:
                start()

        def add_task(task):
            self.tasks.append(task)
            task.connect('progress', progress_cb)
            task.connect('success', success_cb)
            task.connect('status', status_cb)
            task.connect('start', start_cb)
            task.connect('error', error_cb)

        filename = job['filename']
        src = os.path.join(workspace_dir, filename)
        dst = os.path.splitext(filename)[0] + '.m4v'
        dst = os.path.join(workspace_dir, dst)

        def on_transcode_finish(task, src, dst):
            task.job['output']['transcoded'] = os.path.join(output_dir, os.path.basename(dst))
            task.job['output']['stat'] = {}
            # XXX: this may end up with a different inode number after moving to processed dir.
            try:
                task.job['output']['stat'] = stat_to_dict(os.stat(dst))
            except OSError:
                pass
            task.job.save()

        _type = getFileType(src)
        if _type['type'] == 'video':
            task = Transcode(job, src, dst)
            task.connect('finished', on_transcode_finish)
            add_task(task)

            # yeah, looks weird but we want the md5 of the already transcoded file.
            task = MD5(job, src=dst)
            add_task(task)

            # even worse but we want the filmstrip of the already transcoded file.
            src = dst
            task = Filmstrip(job, src)
            add_task(task)

            task = FFmpegInfo(job, src)
            add_task(task)

            task = Thumbnail(job, src)
            add_task(task)

        else:
            task = MD5(job, src)
            add_task(task)

            task = Filmstrip(job, src)
            add_task(task)

            task = FFmpegInfo(job, src)
            add_task(task)

            task = Thumbnail(job, src)
            add_task(task)


        start()

        return True

    def queue_file(self, filepath, do_copy=True):
        try:
            stat = stat_to_dict(os.stat(filepath))
        except OSError:
            return

        filename = os.path.basename(filepath)
        _type = getFileType(filepath)

        if not _type:
            logging.debug('File not recognized: %s', filepath)
            return

        # not reimplementing as mongo is quite good at this.
        if self.queue._col.find({ 'input.stat.mtime': stat['mtime'], 'path': filepath}).count():
            return

        job = Job( {
            'input':    {
                'stat': stat,
                'path': filepath,
            },
            'output':    {
                'checksum': '',
                'files': [],
                'metadata': {
                    'type': _type['type'],
                },
            },
            'filename': filename,
            'stage':    '',
            'progress': '0',
            'tasks':  [], # list of: {name:'', status:'', message:''}
        })

        job.save()

        if do_copy:
            try:
                copy_or_link(filepath, os.path.join(workspace_dir, filename))
                job['stage'] = 'queued'
                os.unlink(filepath)
            except:
                e = sys.exc_info()[1]
                job['stage'] = 'processing-error'
                job['tasks'].append({
                    'name': 'Moving files',
                    'status': 'failed',
                    'message': 'Error: ' + unicode(e),
                })
            finally:
                job.save()
        else:
            job['stage'] = 'queued'
            job.save()

        self.queue.add(job)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    p = Patero()
    m = Monitor(incoming_dir)

    def new_file_cb(monitor, filepath):
        p.queue_file(filepath.decode('utf-8'))

    m.connect('new-file', new_file_cb)

    for fn in os.listdir(incoming_dir):
        afn = os.path.join(incoming_dir, fn)
        if not os.path.isfile(afn):
            continue
        p.queue_file(afn)

    loop = GLib.MainLoop()
    loop.run()
