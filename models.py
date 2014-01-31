from common import *
from backbone import Model, Collection

class Status(Model):
    backend = 'transcodestatus'
    defaults= {
        'running': True,
        '_id': 1
    }

class Job(Model):
    backend = 'transcode'
    colname = 'transcode_queue'
    defaults= {
        'input':    {
            'stat': {},
            'path': '',
        },
        'output':    {
            'checksum': '',
            'files': [],
            'metadata': {
                'type': 'file',
            },
        },
        'filename': '',
        'stage':    '',
        'progress': '0',
        'tasks':  [], # list of: {name:'', status:'', message:''}
    }

class JobCollection(Collection):
    backend = 'transcode'
    colname = 'transcode_queue'
    model   = Job

class Media(Model):
    backend = 'media'
    colname = 'medias'
    defaults= {
        'stat': {},
        'file': "None",
        'name': "",
        'audio': "None",
        'video': "None",
        'checksum': "",
        'durationraw': "",
        'type': "file",
    }

