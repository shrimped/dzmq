#!/usr/bin/env python
from __future__ import print_function
import pybsonmq
import time


d = pybsonmq.DZMQ(tcp_port=55555, ipaddr='127.0.0.1')
d.advertise('status')


SENSOR_MSGS = 0
LOG_MSGS = 0


def write_sensor_data(msg):
    global SENSOR_MSGS
    SENSOR_MSGS += 1


def write_log_data(msg):
    global LOG_MSGS
    LOG_MSGS += 1


d.subscribe('sensor_data', write_sensor_data)
d.subscribe('log', write_log_data)

tlast = time.time()
while True:
    if time.time() - tlast > 1:
        d.publish('status', 'writer ready')
        tlast = time.time()
        print(SENSOR_MSGS, LOG_MSGS)
    time.sleep(0.001)
    d.spinOnce(0.001)
