from .. import DZMQ
import time


class TestPubSub(object):

    def setup(self):
        self.pub = DZMQ()
        self.sub = DZMQ()

    def test_basic(self):
        self.pub.advertise('what_what')
        time.sleep(1.0)

        def cb(topic, msg):
            assert topic == 'what_what'
            raise  # TODO: use logging here

        self.sub.subscribe('what_what', cb)
        self.sub.spinOnce()

        self.pub.publish('what_what', {'foo': 'bar'})
        time.sleep(1.)

        self.sub.spinOnce()
