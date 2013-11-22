# -*- coding: utf-8 -*-

from pymongo import MongoClient
import os, shutil

import config

mongocnstr = config.get('mongodb', 'mongodb://localhost:27017/')
dbname     = config.get('dbName', 'mediadb')
queue_coll = config.get('TranscodingCollection', 'transcode_queue')

redis_host = config.get('redisHost', 'localhost')
redis_port = config.get('redisPort', 6379)
redis_db   = config.get('redisDb',   0)


incoming_dir  = config.get('incoming_dir' , '/media/datos/compartida/patero/incoming')
workspace_dir = config.get('workspace_dir', '/media/datos/compartida/patero/workspace')
output_dir    = config.get('output_dir'   , '/media/datos/compartida/patero/processed')

for folder in [incoming_dir, workspace_dir, output_dir]:
    if not os.path.isdir(folder):
        os.makedirs(folder)

def copy_or_link(source, destination):
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)

def stat_to_dict(s):
    fields = [  'atime', 'blksize', 'blocks', 'ctime',
                'dev', 'gid', 'ino', 'mode', 'mtime',
                'nlink', 'rdev', 'size', 'uid'
    ]

    ret = {}
    for field in fields:
        try:
            ret[field] = getattr(s, 'st_' + field)
        except AttributeError:
            continue

    return ret
