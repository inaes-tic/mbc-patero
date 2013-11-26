#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os, shutil

from gi.repository import GLib, GObject

import redis,json

import pymongo
from pymongo import MongoClient
ObjectId = pymongo.helpers.bson.ObjectId

from common import *
from Melt import Transcode
from monitor import Monitor

class Patero(GObject.GObject):
    def __init__(self):
        GObject.GObject.__init__(self)

        self.client = MongoClient(mongocnstr)
        db = self.db = getattr(self.client, dbname)
        queue = self.queue = getattr(db, queue_coll)

        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        self.melt = None

        res = queue.update({'stage': {'$ne':'processing-done'}}, {'$set': {'stage': 'queued', 'message': [], 'progress':0}}, multi=True)
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
        if self.melt:
            return True

        job = self.queue.find_one({'stage': 'queued'})

        if not job:
            return True

        job['stage'] = 'about-to-process'
        self.save_job(job)

        filename = job['filename']
        src = os.path.join(workspace_dir, filename)
        dst = os.path.splitext(filename)[0] + '.m4v'
        dst = os.path.join(output_dir, dst)

        def progress_cb(melt, progress, job):
            logging.debug('Progress: %s', progress)
            job['progress'] = progress
            self.save_job(job)


        def start_cb(melt, src, dst, job):
            logging.debug('Start: %s', src)
            job['stage'] = 'processing'
            self.save_job(job)

        def error_cb(melt, msg, job):
            # XXX: get rid of all files here?
            logging.error('Error: %s', msg)
            job['stage'] = 'processing-error'
            job['message'].append('Error: ' + msg)
            self.save_job(job)
            self.melt = None

        def stage_cb(melt, job, msg):
            logging.debug('Stage: %s', msg)
            if job['message']:
                job['message'][-1] += '...Done!'
            job['message'].append(msg)
            self.save_job(job)

        def success_cb(melt, dst, job):
            logging.debug('Ok: %s', dst)
            job['stage'] = 'processing-done'
            job['message'].append('All done!')
            self.save_job(job)
            self.melt = None

            src = os.path.join(workspace_dir, job['filename'])
            copy_or_link(src, dst)
            os.unlink(src)
            # XXX FIXME: here tell Caspa the file is ready, pass it thru filmstrip and ffmpeg filters.

        m = self.melt = Transcode(src, dst)
        m.connect('progress', progress_cb, job)
        m.connect('success', success_cb, job)
        m.connect('start', start_cb, job)
        m.connect('error', error_cb, job)
        m.connect('start-audio', stage_cb, job, 'processing-normalize-audio')
        m.connect('start-video', stage_cb, job, 'processing-transcode-video')

        m.start()

        return True

    def queue_file(self, filepath, do_copy=True):
        try:
            stat = stat_to_dict(os.stat(filepath))
        except OSError:
            return

        filename = os.path.basename(filepath)

        db = self.db
        if db.transcode_queue.find({ 'input.stat.ino': stat['ino']}).count():
            return

        job = {
            'input':    {
                'stat': stat,
                'path': filepath,
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
