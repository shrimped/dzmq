#!/usr/bin/env python

import dzmq
import time
import sys

if 'linux' in sys.platform:
    d = dzmq.DZMQ(address='ipc:///tmp/other')
else:
    d = dzmq.DZMQ()

d.advertise('status')
d.advertise('log')
d.advertise('sensor_data')


while not d.get_listeners('log'):
    d.spinOnce(0.001)
print('synched')

i = 0
while i < 10000:
    d.spinOnce(0.0)
    d.publish('sensor_data', 'other data' * 1000)
    time.sleep(0.0005)
    i += 1
    if (i % 10) == 0:
        d.publish('log', 'other says hello')

print(d.get_listeners('log'))
print('done!')
