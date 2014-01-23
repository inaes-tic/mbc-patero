import json
import codecs

__user = {}
__defaults = {}
try:
    fp = codecs.open('config.json', 'r+', 'utf-8')
    __user = json.load(fp)
except ValueError:
# could not decode json, file empty?
    pass
finally:
    fp.close()

try:
    fp = codecs.open('defaults.json', 'r+', 'utf-8')
    __defaults = json.load(fp)
except ValueError:
# could not decode json, file empty?
    pass
finally:
    fp.close()


import atexit
@atexit.register
def __save_config():
    fp = codecs.open('config.json', 'w', 'utf-8')
    json.dump(__defaults, fp, indent=4, sort_keys=True)
    fp.close()


__defaults.update(__user)

config = __defaults
get = config.setdefault
