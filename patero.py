#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os, shutil
from collections import deque

from gi.repository import GLib, GObject

import redis,json

import pymongo
from pymongo import MongoClient
ObjectId = pymongo.helpers.bson.ObjectId

from common import *
from jobs import Transcode, MD5, Filmstrip
from monitor import Monitor

class Patero(GObject.GObject):
    def __init__(self):
        GObject.GObject.__init__(self)

        self.client = MongoClient(mongocnstr)
        db = self.db = getattr(self.client, dbname)
        queue = self.queue = getattr(db, queue_coll)

        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        self.tasks = deque()
        self.running = False

        res = queue.update({'stage': {'$ne':'processing-done'}}, {'$set': {'stage': 'queued', 'message': [], 'progress':0, 'output.files': []}}, multi=True)
        if res['n']:
            self.refresh_jobs()

        for obj in queue.find({'stage': 'moving'}):
            self.delete_job(obj)
            self.queue_file(obj['input']['path'])

        GLib.timeout_add(500, self.transcode)
        GLib.timeout_add(500, self.send_status)


    def send_status(self, status=None):
        if status is None:
            status = { 'running': True, 'id': 1 }
        method = 'created'
        self.redis.publish('Transcode.Status', json.dumps({'method': method, 'payload': status}))
        return True

    def refresh_jobs(self):
        method = 'updated'
        for job in self.queue.find():
            _oid = job['_id']
            _id = unicode(_oid)
            job['_id'] = _id
            job['id']  = _id
            self.redis.publish('Transcode.Progress', json.dumps({'method': method, 'payload': job}))
            job['_id'] = _oid
            job['id']  = _oid

    def delete_job(self, job):
        self.queue.remove(job['_id'])
        _id = unicode(job['_id'])
        self.redis.publish('Transcode.Progress', json.dumps({'method': 'deleted', 'payload': {'id':_id, '_id':_id} }))

    def save_job(self, job):
        if '_id' in job:
            method = 'updated'
        else:
            method = 'created'

        _oid = self.queue.save(job)
        _id = unicode(_oid)
        job['_id'] = _id
        job['id']  = _id
        self.redis.publish('Transcode.Progress', json.dumps({'method': method, 'payload': job}))
        job['_id'] = _oid
        job['id']  = _oid

    def transcode(self):
        if self.running:
            return True

        job = self.queue.find_one({'stage': 'queued'})

        if not job:
            return True

        job['stage'] = 'about-to-process'
        self.save_job(job)

        def progress_cb(task, progress):
            job = task.job
            logging.debug('Progress: %s', progress)
            job['progress'] = progress
            self.save_job(job)


        def start_cb(task, src, dst):
            job = task.job
            logging.debug('Start: %s', src)
            job['stage'] = 'processing'
            job['progress'] = 0
            self.save_job(job)

        def error_cb(task, msg):
            job = task.job
            # XXX: get rid of all files here?
            logging.error('Error: %s', msg)
            job['stage'] = 'processing-error'
            job['message'].append('Error: ' + msg)
            self.save_job(job)
            self.tasks.clear()
            self.running = False

        def status_cb(task, msg):
            job = task.job
            logging.debug('Stage: %s', msg)
            if job['message']:
                job['message'][-1] += '...Done!'
            job['message'].append(msg)
            self.save_job(job)

        def start(*args):
            self.running = True
            task = self.tasks.popleft()
            task.start()

        def success_cb(task, dst):
            job = task.job
            if not self.tasks:
                logging.debug('Ok: %s', dst)
                job['stage'] = 'processing-done'
                if job['message']:
                    job['message'][-1] += '...Done!'
                job['message'].append('All done!')
                job['progress'] = 0

                tmp = []
                for filename in job['output']['files']:
                    copy_or_link(filename, output_dir)
                    os.unlink(filename)
                    tmp.append(os.path.join(output_dir, os.path.basename(filename)))
                job['output']['files'] = tmp
                self.save_job(job)
                # XXX FIXME: here tell Caspa the file is ready
                # not needed I guess, when we get there a processing-done we know it's time to add it.
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
            self.save_job(task.job)

        task = Transcode(job, src, dst)
        task.connect('finished', on_transcode_finish)
        add_task(task)

        # yeah, looks weird but we want the md5 of the already transcoded file.
        task = MD5(job, src=dst)
        add_task(task)

        # even worse but we want the filmstrip of the already transcoded file.
        src = dst
        dst = os.path.splitext(filename)[0] + '.mp4'
        dst = os.path.join(workspace_dir, dst)

        task = Filmstrip(job, src, dst)
        add_task(task)


        start()

        return True

    def queue_file(self, filepath, do_copy=True):
        try:
            stat = stat_to_dict(os.stat(filepath))
        except OSError:
            return

        filename = os.path.basename(filepath)

        db = self.db
        if db.transcode_queue.find({ 'input.stat.mtime': stat['mtime'], 'path': filepath}).count():
            return

        job = {
            'input':    {
                'stat': stat,
                'path': filepath,
            },
            'output':    {
                'checksum': '',
                'files': [],
                'metadata': {

                },
            },
            'filename': filename,
            'stage':    'moving',
            'progress': '0',
            'message':  [],
        }
        self.save_job(job)

        if do_copy:
            try:
                copy_or_link(filepath, os.path.join(workspace_dir, filename))
                job['stage'] = 'queued'
                self.save_job(job)
                os.unlink(filepath)
            except:
                job['message'].append('Error moving file')
                self.save_job(job)
        else:
            job['stage'] = 'queued'
            self.save_job(job)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    p = Patero()
    m = Monitor(incoming_dir)

    def new_file_cb(monitor, filepath):
        p.queue_file(filepath)

    m.connect('new-file', new_file_cb)

    for fn in os.listdir(incoming_dir):
        afn = os.path.join(incoming_dir, fn)
        if not os.path.isfile(afn):
            continue
        p.queue_file(afn)

    loop = GLib.MainLoop()
    loop.run()
