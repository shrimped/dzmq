#!/usr/bin/env python


import pybsonmq
import time

d = pybsonmq.DZMQ(tcp_port=55545, ipaddr='127.0.0.1')
d.advertise('status')
d.advertise('log')
d.advertise('sensor_data')


while not d.listeners['log']:
    d.spinOnce(0.001)
time.sleep(0.25)


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
