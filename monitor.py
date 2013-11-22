import pyinotify
from gi.repository import GLib, GObject

class EventHandler(pyinotify.ProcessEvent, GObject.GObject):
    __gsignals__ = {
        'new-file': (GObject.SIGNAL_RUN_FIRST, None, [str]),
    }

    def __init__(self, *args, **kwargs):
        pyinotify.ProcessEvent.__init__(self, *args, **kwargs)
        GObject.GObject.__init__(self)

    def process_default(self, event):
        if event.dir:
            return

        self.emit('new-file', event.pathname)

class Monitor(GObject.GObject):
    __gsignals__ = {
        'new-file': (GObject.SIGNAL_RUN_FIRST, None, [str]),
    }

    def __init__(self, path=None):
        GObject.GObject.__init__(self)

        self.wm = pyinotify.WatchManager()
        self.handler=EventHandler()

        self.handler.connect('new-file', self._emit_new_file)

        self.notifier = pyinotify.Notifier(self.wm, timeout=10, default_proc_fun=self.handler)

        if path:
            self.add_path(path)

        GLib.idle_add(self._process_events)

    def add_path(self, path):
        self.wm.add_watch(path,
            pyinotify.IN_CLOSE_WRITE |
            pyinotify.IN_MOVED_TO
        )

    def _emit_new_file(self, handler, filepath):
        self.emit('new-file', filepath)

    def _process_events(self):
        notifier = self.notifier
        notifier.process_events()
        while notifier.check_events():
            notifier.read_events()
            notifier.process_events()
        return True

if __name__ == '__main__':
    def file_cb(monitor, filepath):
        print 'New file: ', filepath

    m = Monitor('/dev/shm')
    m.connect('new-file', file_cb)

    loop = GLib.MainLoop()
    loop.run()
