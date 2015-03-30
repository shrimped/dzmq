#!/usr/bin/env python

import sys
import dzmq

if len(sys.argv) > 1:
    topic = sys.argv[1]
else:
    topic = 'foo'


def cb1(msg):
    print('1: Got %s' % msg)


def cb2(msg):
    print('2: Got %s' % msg)

d = dzmq.DZMQ()
d.subscribe(topic, cb1)
d.subscribe(topic, cb2)
d.spin()
