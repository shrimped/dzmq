from .. import DZMQ
import time
import logging
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class TestPubSub(object):

    def setup(self):
        self.pub = DZMQ()
        self.sub = DZMQ()
        self.sub.log.setLevel(logging.DEBUG)
        sobj = StringIO()
        hdlr = logging.StreamHandler(sobj)
        hdlr.setLevel(logging.DEBUG)
        self.sub.log.addHandler(hdlr)
        self.io_hdlr = hdlr

    def test_basic(self):
        self.pub.advertise('what_what')
        time.sleep(1.0)
        payload = {'foo': 'bar'}

        def cb(topic, msg):
            assert topic == 'what_what'
            assert msg == payload

        self.sub.subscribe('what_what', cb)
        self.sub.spinOnce()

        self.pub.publish('what_what', payload)
        time.sleep(1.)
        self.sub.spinOnce()

        # check the output
        output = self.io_hdlr.stream.getvalue().strip()
        assert "Connected to" in output
        assert "Got message: what_what" in output

        self.io_hdlr.stream.truncate(0)
