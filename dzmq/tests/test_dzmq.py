from dzmq import DZMQ

import logging
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
try:
    import numpy as np
except ImportError:
    np = None

import unittest
import tempfile
import os


class TestPubSub(unittest.TestCase):

    def setUp(self):
        self.pub = DZMQ()

        self.sub = DZMQ()
        self.sub.log.setLevel(logging.DEBUG)

        sobj = StringIO()
        hdlr = logging.StreamHandler(sobj)
        hdlr.setLevel(logging.DEBUG)
        self.sub.log.addHandler(hdlr)
        self.io_hdlr = hdlr

    def get_log(self):
        output = self.io_hdlr.stream.getvalue().strip()
        self.io_hdlr.stream.truncate(0)
        return output

    def synch(self, topic):
        while not self.pub.get_listeners(topic):
            self.sub.spinOnce()
            self.pub.spinOnce()

    def test_basic(self):
        self.pub.advertise('what_what')
        payload = {'foo': 'bar'}

        def cb(msg):
            assert msg == payload, msg

        self.sub.subscribe('what_what', cb)
        self.synch('what_what')

        self.pub.publish('what_what', payload)
        self.sub.spinOnce()
        self.sub.spinOnce()

        # check the output
        output = self.get_log()
        assert "Connected to" in output
        assert "Got message: what_what" in output, output

    def test_multiple_topics(self):
        self.pub.advertise('hey_hey')
        self.pub.advertise('boo_boo')
        payload = {'spam': 100}

        def cb(msg):
            assert msg == payload

        self.sub.subscribe('hey_hey', cb)

        self.synch('hey_hey')
        self.pub.publish('hey_hey', payload)
        self.pub.publish('boo_boo', payload)
        self.sub.spinOnce()
        self.sub.spinOnce()

        # check the output
        output = self.get_log()
        assert "Connected to" in output
        assert "Got message: hey_hey" in output

    def test_multiple_cbs(self):
        self.pub.advertise('yeah_yeah')
        if np:
            payload = {'eggs': np.random.random(100)}
        else:
            payload = {'eggs': [1, 2, 3]}

        def cb1(msg):
            self.sub.log.debug('Got cb1')
            assert 'eggs' in msg

        def cb2(msg):
            self.sub.log.debug('Got cb2')
            assert 'eggs' in msg

        self.sub.subscribe('yeah_yeah', cb1)
        self.sub.subscribe('yeah_yeah', cb2)

        self.synch('yeah_yeah')
        self.pub.publish('yeah_yeah', payload)

        self.sub.spinOnce()
        self.sub.spinOnce()

        # check the output
        output = self.get_log()
        assert "Connected to" in output
        assert "Got message: yeah_yeah" in output
        assert 'Got cb1' in output
        assert 'Got cb2' in output

    def test_unadvertise(self):
        self.pub.advertise('yeah_yeah')
        payload = {'spam': 100}

        def cb(msg):
            assert False

        self.sub.subscribe('yeah_yeah', cb)
        self.synch('yeah_yeah')

        self.pub.unadvertise('yeah_yeah')
        self.assertRaises(ValueError, self.pub.publish, 'yeah_yeah', payload)

    def test_unsubscribe(self):
        self.pub.advertise('yeah_yeah')
        payload = {'spam': 100}

        def cb(msg):
            assert msg == payload, msg

        self.sub.subscribe('yeah_yeah', cb)

        self.synch('yeah_yeah')
        self.pub.publish('yeah_yeah', payload)
        self.sub.spinOnce()
        self.sub.spinOnce()

        # check the output
        output = self.get_log()
        assert "Connected to" in output
        assert "Got message: yeah_yeah" in output, output

        self.sub.spinOnce()
        self.sub.unsubscribe('yeah_yeah')
        self.sub.spinOnce()

        self.pub.publish('yeah_yeah', payload)
        self.sub.spinOnce()

        # check the output
        output = self.get_log()
        assert "Got message: yeah_yeah" not in output, output

    def test_idle_cb(self):
        self._temp = 0

        def cb():
            self._temp = 1

        self.sub.register_cb(cb)
        self.sub.spinOnce()
        assert self._temp == 1

    def test_poll_cb(self):
        if os.name == 'nt':
            return

        dname = tempfile.mkdtemp()
        fname = os.path.join(dname, 'test.pipe')
        os.mkfifo(fname)

        r = os.open(fname, os.O_RDONLY | os.O_NONBLOCK)
        w = os.open(fname, os.O_WRONLY | os.O_NONBLOCK)

        def cb():
            assert os.read(r, 10) == b'hello\n'

        self.pub.register_cb(cb, r)
        os.write(w, b'hello\n')
        self.pub.spinOnce()

        os.write(w, b'goodbye\n')
        assert os.read(r, 10) == b'goodbye\n'

    def teardown(self):
        self.pub.close()
        self.sub.close()
