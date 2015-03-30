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


while not d.get_listeners('log'):
    d.spinOnce(0.001)
print('synched')

i = 0
while i < 10000:
    d.spinOnce(0.0)
    d.publish('sensor_data', 'other data' * 1000)
    time.sleep(0.0015)
    i += 1
    if (i % 10) == 0:
        d.publish('log', 'other says hello')

print(d.get_listeners('log'))
print('done!')
