import socket
import errno
import uuid
import os
import logging
import struct
import atexit
from collections import defaultdict
import sys
import time
import signal
import pickle

import zmq

from .utils import get_log, get_local_addresses


# Defaults and overrides
BCAST_PORT = 11312
MULTICAST_GRP = '225.25.25.25'
DZMQ_PORT_KEY = 'DZMQ_BCAST_PORT'
DZMQ_HOST_KEY = 'DZMQ_BCAST_HOST'
DZMQ_IP_KEY = 'DZMQ_IP'
DZMQ_IFACE_KEY = 'DZMQ_IFACE'

# Constants
OP_ADV = 0x01
OP_SUB = 0x02

UDP_MAX_SIZE = 512
GUID_LENGTH = 8
HB_REPEAT_PERIOD = 1.0
VERSION = 0x0001
TOPIC_MAXLENGTH = 192
FLAGS_LENGTH = 16
ADDRESS_MAXLENGTH = 267
DEBUG = False


class Broadcaster(object):

    def __init__(self, log):
        self.log = log
        self.guid = uuid.uuid4()
        # Determine network addresses.  Look at environment variables, and
        # fall back on defaults.

        # What IP address will we give to others to use when contacting us?
        addrs = []
        env = os.environ
        if 'DZMQ_USE_LOOPBACK' not in env:
            if DZMQ_IP_KEY in env:
                addrs = get_local_addresses(addrs=[env[DZMQ_IP_KEY]])
            elif DZMQ_IFACE_KEY in env:
                addrs = get_local_addresses(ifaces=[env[DZMQ_IFACE_KEY]])
            if not addrs:
                addrs = get_local_addresses()
        if addrs:
            self.ipaddr = addrs[0]['addr']
            self.host = addrs[0]['broadcast']
        else:
            if sys.platform == 'win32':
                self.ipaddr = '127.0.0.1'
                self.host = '255.255.255.255'
            elif 'linux' in sys.platform:
                self.ipaddr = '127.0.0.255'
                self.host = '127.255.255.255'
            else:
                self.ipaddr = '127.0.0.1'
                self.host = MULTICAST_GRP

        # What's our broadcast port?
        if DZMQ_PORT_KEY in env:
            self.port = int(env[DZMQ_PORT_KEY])
        else:
            # Take the default
            self.port = BCAST_PORT
        # What's our broadcast host?
        if DZMQ_HOST_KEY in env:
            self.host = env[DZMQ_HOST_KEY]

        # Set up to send broadcasts
        self.sock = socket.socket(socket.AF_INET,  # Internet
                                        socket.SOCK_DGRAM,  # UDP
                                        socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sys.platform == 'darwin':
            self.sock.setsockopt(socket.SOL_SOCKET,
                                       socket.SO_REUSEPORT, 1)
        self.sock.bind(('', self.port))

        if self.host == MULTICAST_GRP:
            self.sock.setsockopt(socket.IPPROTO_IP,
                                       socket.IP_MULTICAST_TTL, 2)
            mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GRP),
                               socket.INADDR_ANY)
            self.sock.setsockopt(socket.IPPROTO_IP,
                                       socket.IP_ADD_MEMBERSHIP,
                                       mreq)
        self.pubs = dict()
        self.log.info("Opened (%s, %s)" %
                      (self.host, self.port))

    def advertise(self, topic, address):
        msg = self.pubs.get(topic, None)
        if not msg:
            msg = b''
            msg += struct.pack('<H', VERSION)
            msg += self.guid.bytes[:GUID_LENGTH]
            msg += struct.pack('<B', len(topic))
            msg += topic.encode('utf-8')
            msg += struct.pack('<B', OP_ADV)
            # Flags unused for now
            flags = [0x00] * FLAGS_LENGTH
            msg += struct.pack('<%dB' % (FLAGS_LENGTH), *flags)
            msg += struct.pack('<H', len(address))
            msg += address.encode('utf-8')
            self.pubs[topic] = msg
        self.sock.sendto(msg, (self.host, self.port))

    def subscribe(self, topic, address):
        msg = b''
        msg += struct.pack('<H', VERSION)
        msg += self.guid.bytes[:GUID_LENGTH]
        msg += struct.pack('<B', len(topic))
        msg += topic.encode('utf-8')
        msg += struct.pack('<B', OP_SUB)
        # Flags unused for now
        flags = [0x00] * FLAGS_LENGTH
        msg += struct.pack('<%dB' % FLAGS_LENGTH, *flags)
        msg += struct.pack('<H', len(address))
        msg += address.encode('utf-8')
        self.sock.sendto(msg, (self.host, self.port))

    def handle_recv(self):
        """
        Internal method to handle receipt of broadcast messages.
        """
        msg = self.sock.recvfrom(UDP_MAX_SIZE)
        data, addr = msg
        # Unpack the header
        offset = 0
        version = struct.unpack_from('<H', data, offset)[0]
        if version != VERSION:
            self.log.warn('Warning: mismatched protocol versions: %d != %d'
                          % (version, VERSION))
        offset += 2
        guid = data[offset: offset + GUID_LENGTH]
        if guid == self.guid.bytes[:GUID_LENGTH]:
            return
        offset += GUID_LENGTH

        topiclength = struct.unpack_from('<B', data, offset)[0]
        offset += 1
        topic = data[offset:offset + topiclength].decode('utf-8')
        offset += topiclength
        op = struct.unpack_from('<B', data, offset)[0]
        offset += 1
        flags = struct.unpack_from('<%dB' % FLAGS_LENGTH, data, offset)
        offset += FLAGS_LENGTH

        if op == OP_ADV:
            # Unpack the ADV body
            adv = {}
            adv['topic'] = topic
            adv['type'] = 'adv'
            adv['guid'] = guid
            adv['flags'] = flags
            addresslength = struct.unpack_from('<H', data, offset)[0]
            offset += 2
            addr = data[offset:offset + addresslength]
            adv['addr'] = addr.decode('utf-8')
            offset += addresslength

            return adv

        elif op == OP_SUB:
            # Unpack the sub body
            sub = {}
            sub['type'] = 'sub'
            sub['topic'] = topic
            sub['guid'] = guid
            sub['flags'] = flags
            addresslength = struct.unpack_from('<H', data, offset)[0]
            offset += 2
            addr = data[offset:offset + addresslength]
            sub['addr'] = addr.decode('utf-8')
            offset += addresslength

            return sub

        else:
            self.log.warn('Warning: got unrecognized OP: %d' % op)

    def unadvertise(self, topic):
        self.pubs.pop(topic, None)

    def send_all(self):
        for msg in self.pubs.values():
            self.sock.sendto(msg, (self.host, self.port))


class Publisher(object):

    def __init__(self, socket, log):
        self.sock = socket
        self.log = log
        self.sock.setsockopt(zmq.LINGER, 0)
        self.listeners = defaultdict(dict)

    def advertise(self, topic):
        self.listeners[topic] = dict()

    def unadvertise(self, topic):
        self.listeners.pop(topic, None)

    def publish(self, topic, msg):
        packed_msg = pickle.dumps(msg, protocol=-1)
        self.sock.send_multipart((topic.encode('utf-8'), packed_msg))

    def get_listeners(self, topic):
        if topic not in self.listeners:
            return []
        return [addr for (addr, tstamp) in self.listeners[topic].items()
                if (time.time() - tstamp) < 2 * HB_REPEAT_PERIOD]


class Subscriber(object):

    def __init__(self, socket, log):
        self.sock = socket
        self.log = log
        self.sock.setsockopt(zmq.LINGER, 0)
        self.subs = dict()
        self.conns = []
        self.status = defaultdict(list)

    def subscribe(self, topic, cb):
        if topic not in self.subs:
            self.subs[topic] = []
        self.subs[topic].append(cb)

    def unsubscribe(self, topic):
        self.subs.pop(topic, None)

        status = dict()
        for (addr, topics) in self.status.items():
            status[addr] = [t for t in topics if not t == topic]

        self.status = status

    def handle_adv(self, adv):
        """
        Internal method to connect to a publisher.
        """
        if adv['topic'] not in self.subs:
            return

        # Are we already connected to this publisher for this topic?
        if [c for c in self.conns
                if c['topic'] == adv['topic'] and c['guid'] == adv['guid']]:
            return

        # Are we already connected to this publisher for this topic
        # on this address?
        for c in self.conns:
            if c['topic'] == adv['topic'] and c['addr'] == adv['addr']:
                c['guid'] == adv['guid']
                return

        # Connect our subscriber socket
        conn = {}
        conn['socket'] = self.sock
        conn['topic'] = adv['topic']
        conn['addr'] = adv['addr']
        conn['guid'] = adv['guid']
        conn['socket'].setsockopt(zmq.SUBSCRIBE, adv['topic'].encode('utf-8'))

        self.status[adv['addr']].append(adv['topic'])

        if not any([s['addr'] == adv['addr'] for s in self.conns]):
            conn['socket'].connect(adv['addr'])

        self.conns.append(conn)
        self.log.info('Connected to %s for %s' %
                      (adv['addr'], adv['topic']))
        return True

    def handle_recv(self):
        # Get the message (assuming that we get it all in one read)
        topic, msg = self.sock.recv_multipart()
        topic = topic.decode('utf-8')
        if topic not in self.subs:
            return
        msg = pickle.loads(msg)
        [cb(msg) for cb in self.subs[topic]]
        self.log.debug('Got message: %s' % topic)


class DZMQ(object):

    """
    This class provides a basic pub/sub system with discovery.  Basic
    publisher:

    import pybsonmq
    d = pybsonmq.DZMQ()
    d.advertise('foo')
    msg = 'bar'
    while True:
        d.publish('foo', msg)
        d.spinOnce(0.1)

    Basic subscriber:

    from __future__ import print_function
    import pybsonmq
    d = pybsonmq.DZMQ()
    d.subscribe('foo', lambda topic,msg: print('Got %s on %s'%(topic,msg)))
    d.spin()

    """

    def __init__(self, context=None, log=None, address=None):
        """ Initialize the DZMQ interface

        Parameters
        ----------
        context : zmq.Context, optional
            zmq Context instance.
        log : logging.Logger
            Logger instance.
        address : str
            Valid ZMQ address: tcp:// or ipc://.
        """
        self._context = context or zmq.Context.instance()
        self.log = log or get_log()
        self.address = address

        atexit.register(self.close)

        if DEBUG:
            self.log.setLevel(logging.DEBUG)

        self._broadcast = Broadcaster(self.log)

        signal.signal(signal.SIGINT, self._sighandler)

        # Set up the one pub and one sub socket that we'll use
        pub_socket = self._context.socket(zmq.PUB)
        if not self.address:
            tcp_addr = 'tcp://%s' % (self._broadcast.ipaddr)
            tcp_port = pub_socket.bind_to_random_port(tcp_addr)
            tcp_addr += ':%d' % (tcp_port)
            self.address = tcp_addr
        else:
            pub_socket.bind(self.address)

        if len(self.address) > ADDRESS_MAXLENGTH:
            raise Exception('Address length %d exceeds maximum %d'
                            % (len(self.address), ADDRESS_MAXLENGTH))

        self._publisher = Publisher(pub_socket, self.log)

        sub_socket = self._context.socket(zmq.SUB)
        self._subscriber = Subscriber(sub_socket, self.log)

        self._poller = zmq.Poller()
        self._poller.register(self._broadcast.sock, zmq.POLLIN)
        self._poller.register(sub_socket, zmq.POLLIN)

        self._poll_cbs = dict()
        self._idle_cbs = []
        self._local_subs = dict()

        self._last_hb = 0

        self.advertise('_heartbeat')
        self.subscribe('_heartbeat', self._handle_hb)

    def _handle_hb(self, msg):
        if self.address not in msg['subs']:
            return
        listeners = self._publisher.listeners
        addr = msg['address']
        for topic in msg['subs'][self.address]:
            if topic in listeners:
                listeners[topic][addr] = time.time()

    def _sighandler(self, sig, frame):
        self.close()

    def advertise(self, topic):
        """
        Advertise the given topic.  Do this before calling publish().

        Parameters
        ----------
        topic : str
            Topic name.
        """
        if len(topic) > TOPIC_MAXLENGTH:
            raise Exception('Topic length %d exceeds maximum %d'
                            % (len(topic), TOPIC_MAXLENGTH))

        self._broadcast.advertise(topic, self.address)
        self._publisher.advertise(topic)

        if topic not in self._local_subs:
            self._local_subs[topic] = []

    def unadvertise(self, topic):
        """
        Unadvertise a topic.

        Parameters
        ----------
        topic : str
            Topic name.
        """
        self._publisher.unadvertise(topic)
        self._broadcast.unadvertise(topic)

    def subscribe(self, topic, cb):
        """
        Subscribe to the given topic.  Received messages will be passed to
        given the callback, which should have the signature: cb(msg).

        topic : str
            Name of topic.
        cb : callable
            Callable that accepts one argument (msg).
        """
        self._subscriber.subscribe(topic, cb)
        self._broadcast.subscribe(topic, self.address)

        if topic not in self._local_subs:
            self._local_subs[topic] = []

        self._local_subs[topic].append(cb)

    def unsubscribe(self, topic):
        """
        Unsubscribe from a given topic.

        Parameters
        ----------
        topic : str
            Name of topic.
        """
        self._subscriber.unsubscribe(topic)
        self._local_subs.pop(topic, None)

    def publish(self, topic, msg):
        """
        Publish the given message on the given topic.  You should have called
        advertise() on the topic first.

        Parameters
        ----------
        topic : str
            Name of topic.
        msg : str or dict
            Mesage to send.
        """
        if topic not in self._publisher.listeners:
            raise ValueError('"%s" is not an advertised topic' % topic)

        self._publisher.publish(topic, msg)
        [cb(msg) for cb in self._local_subs[topic]]

    def get_listeners(self, topic):
        """
        Get a list of current listeners for a given topic.

        Parameters
        ----------
        topic : str
            Name of topic.
        """
        return self._publisher.get_listeners(topic)

    def register_cb(self, cb, obj=None):
        """Register a callback to the spinOnce loop.

        Parameters
        ----------
        cb : callable
            Callback to register.
        obj : A zmq.Socket, an integer fd or have a ``fileno()`` method.
            If not given, call when idle.
        """
        if not obj:
            self._idle_cbs.append(cb)
            return

        print(obj, repr(obj))
        self._poller.register(obj, zmq.POLLIN)
        self._poll_cbs[obj] = cb

    def _handle_bcast_recv(self):

        data = self._broadcast.handle_recv()
        if not data:
            return

        topic = data['topic']
        addr = data['addr']

        if data['type'] == 'sub':
            listeners = self._publisher.listeners
            if topic in listeners:
                if addr not in listeners[topic]:
                    self._broadcast.advertise(topic, self.address)

        elif data['type'] == 'adv':
            if self._subscriber.handle_adv(data):
                self._send_hb()

    def _send_hb(self):

        self._last_hb = time.time()
        self._broadcast.send_all()
        msg = dict(address=self.address,
                   subs=self._subscriber.status)
        self._publisher.publish('_heartbeat', msg)

    def spinOnce(self, timeout=0.01):
        """
        Check for incoming messages, invoking callbacks.

        Parameters
        ----------
        timeout : float
            Timeout in seconds. Wait for up to timeout seconds.  For no
            waiting, set timeout=0. To wait forever, set timeout=-1.
        """
        if timeout < 0:
            # zmq interprets timeout=None as infinite
            timeout = None
        else:
            # zmq wants the timeout in milliseconds
            timeout = int(timeout * 1e3)

        # Look for sockets that are ready to read
        try:
            items = dict(self._poller.poll(timeout))
        except zmq.ZMQError as e:
            if e.errno == errno.EINTR:
                raise KeyboardInterrupt
            else:
                raise

        for (obj, poll_cb) in self._poll_cbs.items():
            if obj in items or getattr(obj, 'fileno', None) in items:
                poll_cb()

        if items.get(self._broadcast.sock.fileno(), None) == zmq.POLLIN:
            self._handle_bcast_recv()

        if items.get(self._subscriber.sock, None) == zmq.POLLIN:
            self._subscriber.handle_recv()

        if (time.time() - self._last_hb) > HB_REPEAT_PERIOD:
            self._send_hb()

        if items and timeout:
            self.spinOnce(timeout=0)  # avoid a context switch

        if not items or timeout:
            [cb() for cb in self._idle_cbs]

    def spin(self):
        """
        Give control to the message event loop.
        """
        while True:
            try:
                self.spinOnce()
            except KeyboardInterrupt:
                self.close()
                return

    def close(self):
        """
        Close the DZMQ Interface and all of its ports.
        """
        self._broadcast.sock.close()
        self._publisher.sock.close()
        self._subscriber.sock.close()
