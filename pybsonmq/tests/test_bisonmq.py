
import time
import multiprocessing


def pub_server():
    from .. import DZMQ
    time.sleep(1.)
    pub = DZMQ()
    time.sleep(1.)
    pub.advertise('what_what')
    with open('log.txt', 'w') as fid:
        fid.write('%s\n' % pub.pub_socket_addrs)
    while 1:
        pub.publish('what_what', {'foo': 'bar'})
        time.sleep(0.1)


def test_simple():

    proc = multiprocessing.Process(target=pub_server)
    proc.start()

    from .. import DZMQ
    sub = DZMQ()

    def cb(topic, msg):
        assert topic == 'what_what'
        raise

    time.sleep(1.)
    sub.spinOnce()
    sub.subscribe('what_what', cb)
    time.sleep(0.5)
    sub.spinOnce()
    time.sleep(0.5)
    sub.spinOnce()
    time.sleep(0.5)
    sub.spinOnce()
    raise ValueError
