from gi.repository import GObject, GLib

import subprocess
import os
import re

class Handler (GObject.GObject):
    __gsignals__ = {
        'exit': (GObject.SIGNAL_RUN_FIRST, None,
                         (int,)),
        'stderr': (GObject.SIGNAL_RUN_FIRST, None,
                         (str,)),
        'stdout': (GObject.SIGNAL_RUN_FIRST, None,
                         (str,))
    }

    def __init__(self, p=None):
        GObject.GObject.__init__(self)
        self.running = False
        self.next_cmd = []
        self.cbchain = []

        if p:
            self.run(p)

    def nonblock (self, fd):
        import fcntl

        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    def run(self, command):
        if self.running:
            self.then (command)
            return False

        print "Running: " + ' '.join (command)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.process = process

        # make stderr a non-blocking file
        self.nonblock (self.process.stderr.fileno())
        self.nonblock (self.process.stdout.fileno())

        self.running = True
        GLib.timeout_add (200, self.check_runing)
        return True

    def then (self, command):
        self.next_cmd.append(command)

    def next (self):
        try:
            n = self.next_cmd.pop()
            n()
        except:
            print "error no callback"

    def check_line (self, fd):
        if fd == 'stderr':
            out = self.process.stderr
        elif fd == 'stdout':
            out = self.process.stdout
        else:
            return None

        try:
            line = out.read()
            self.emit(fd, line)
        except:
            return None

        return line

    def check_runing (self):
        ret = self.process.poll()

        self.check_line ('stderr')
        self.check_line ('stdout')

        if ret == None:
            return True

        self.emit ('exit', ret)
        self.running = False

#        try:
#            next_cmd = self.next_cmd.pop()
#            self.run(next_cmd)
#        except:
#            pass

        return False

def dump (s, d, e):
    print "DUMP:", e, s, d

if __name__ == "__main__":
    loop = GLib.MainLoop()

    p = Handler()
    p.run(['cat'])
    p.run(['ls', '/'])
    p.run(['ls', '/boot'])
    p.connect('stdout', dump, 'stdout')
    p.connect('exit', dump, 'exit')

    loop.run()
