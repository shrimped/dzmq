#!/usr/bin/env python
from __future__ import print_function
import dzmq
import time
import sys


class Sink(object):

    def __init__(self):

        if 'linux' in sys.platform:
            self.sock = dzmq.DZMQ(address='ipc:///tmp/sink')
        else:
            self.sock = dzmq.DZMQ()

        self.count = 0
        self.ncounts = 1e6
        self.t0 = 0

        self.sock.subscribe('sensor_data', self.handle_data)

    def handle_data(self, msg):
        if not self.count:
            self.t0 = time.time()
        self.count += 1
        if msg is None:
            elapsed = time.time() - self.t0
            print('%s msg, %.3f sec, %.3f msg/sec' % (self.count, elapsed,
                                                      self.count / elapsed))
            self.sock.close()
            sys.exit(0)


if __name__ == '__main__':
    s = Sink()
    s.sock.spin()
