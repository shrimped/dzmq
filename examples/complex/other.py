#!/usr/bin/env python


import pybsonmq
import time

d = pybsonmq.DZMQ(tcp_port=55545, ipaddr='127.0.0.255')
d.advertise('status')
d.advertise('log')
d.advertise('sensor_data')


READY = 0


def check_status(msg):
    global READY
    if msg.lower() == 'writer ready':
        if READY == 0:
            READY = time.time()
        d.publish('status', 'other ready')

d.subscribe('status', check_status)

i = 0
while i < 10000:
    d.spinOnce(0.0)
    if READY and (time.time() - READY) > 1:
        time.sleep(0.0001)
        d.publish('sensor_data', 'other data' * 10)
        i += 1
        if (i % 10) == 0:
            d.publish('log', 'other says hello')


print('done!')
