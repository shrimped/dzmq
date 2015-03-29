#!/usr/bin/env python


import pybsonmq
import time
import sys

if 'linux' in sys.platform:
    d = pybsonmq.DZMQ(address='ipc:///tmp/other')
else:
    d = pybsonmq.DZMQ()

d.advertise('status')
d.advertise('log')
d.advertise('sensor_data')


while not d.listeners['log']:
    d.spinOnce(0.001)
print('synched')

i = 0
while i < 10000:
    d.spinOnce(0.0)
    d.publish('sensor_data', 'other data' * 10)
    time.sleep(0.001)
    i += 1
    if (i % 10) == 0:
        d.publish('log', 'other says hello')

print(d.listeners)
print('done!')
