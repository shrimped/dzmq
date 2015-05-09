#!/usr/bin/env python

import dzmq
import sys
import time


if __name__ == '__main__':

    if 'linux' in sys.platform:
        d = dzmq.DZMQ(address='ipc:///tmp/source')
    else:
        d = dzmq.DZMQ()

    d.advertise('sensor_data')

    while not d.get_listeners('sensor_data'):
        d.spinOnce()
    time.sleep(0.001)  # wait for sink to get our advertisement back

    i = 0
    while i < 1000:
        d.publish('sensor_data', 'other data' * 100)
        d.spinOnce(0.0001)
        i += 1
    d.publish('sensor_data', None)

    print(d.get_listeners('sensor_data'))
    print('done!')
