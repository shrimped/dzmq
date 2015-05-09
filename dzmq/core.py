import socket
import zmq
import uuid
import os
import logging
import platform
import struct
import atexit
from collections import defaultdict
import sys
import time
import netifaces
import signal
import pickle

from .utils import get_log


# Defaults and overrides
BCAST_PORT = 11312
MULTICAST_GRP = '224.1.1.1'
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



# TODO: 
# x - broadcast has a list of brodcast messages, instead of composing them 
# x- remove inproc in favor of local callbacks 
# bring back the raw message send/recv


class Broadcast(object):

    def __init__(self, log):
        self.log = log
        self.guid = uuid.uuid4()
        # Determine network addresses.  Look at environment variables, and
        # fall back on defaults.

        # What IP address will we give to others to use when contacting us?
        addrs = []
        if DZMQ_IP_KEY in os.environ:
            addrs = get_local_addresses(addrs=[os.environ[DZMQ_IP_KEY]])
        elif DZMQ_IFACE_KEY in os.environ:
            addrs = get_local_addresses(ifaces=[os.environ[DZMQ_IFACE_KEY]])
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
        if DZMQ_PORT_KEY in os.environ:
            self.port = int(os.environ[DZMQ_PORT_KEY])
        else:
            # Take the default
            self.port = BCAST_PORT
        # What's our broadcast host?
        if DZMQ_HOST_KEY in os.environ:
            self.host = os.environ[DZMQ_HOST_KEY]
        else:
            pass

        # Set up to send broadcasts
        self.sock = socket.socket(socket.AF_INET,  # Internet
                                        socket.SOCK_DGRAM,  # UDP
                                        socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if platform.system() in ['Darwin']:
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
        self.advert_cache = dict()
        self.sub_cache = dict()
        self.log.info("Opened (%s, %s)" %
                      (self.host, self.port))

    def advertise(self, topic, address):
        msg = self.advert_cache.get((topic, address), None)
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
            self.advert_cache[(topic, address)] = msg
        self.sock.sendto(msg, (self.host, self.port))

    def subscribe(self, topic, address):
        msg = self.sub_cache.get((topic, address), None)
        if not msg:
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
            self.sub_cache[(topic, address)] = msg
        self.sock.sendto(msg, (self.host, self.port))

    def handle_recv(self):
        """
        Internal method to handle receipt of broadcast messages.
        """
        msg = self.sock.recvfrom(UDP_MAX_SIZE)
        try:
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

        except Exception as e:
            self.log.exception(e)
            self.log.warn('Warning: exception while processing SUB or ADV '
                          'message: %s' % e)


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
        self.context = context or zmq.Context.instance()
        self.log = log or get_log()

        atexit.register(self.close)

        if DEBUG:
            self.log.setLevel(logging.DEBUG)

        self.address = address
        self._broadcast = Broadcast(self.log)

        # Bookkeeping (which should be cleaned up)
        self.publishers = []
        self.subscribers = []
        self.sub_connections = []
        self.poller = zmq.Poller()
        self._listeners = defaultdict(dict)

        signal.signal(signal.SIGINT, self._sighandler)

        # Set up the one pub and one sub socket that we'll use
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket_addrs = []
        if not self.address:
            tcp_addr = 'tcp://%s' % (self._broadcast.ipaddr)
            tcp_port = self.pub_socket.bind_to_random_port(tcp_addr)
            tcp_addr += ':%d' % (tcp_port)
            self.address = tcp_addr
        else:
            self.pub_socket.bind(self.address)

        if len(self.address) > ADDRESS_MAXLENGTH:
            raise Exception('Address length %d exceeds maximum %d'
                            % (len(self.address), ADDRESS_MAXLENGTH))
        self.pub_socket_addrs.append(self.address)
        self.pub_socket.setsockopt(zmq.LINGER, 0)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt(zmq.LINGER, 0)
        self.sub_socket_addrs = []

        self.poller.register(self._broadcast.sock, zmq.POLLIN)
        self.poller.register(self.sub_socket, zmq.POLLIN)

        self.poll_cbs = dict()
        self.idle_cbs = []

        # wait for the pub socket to start up
        poller = zmq.Poller()
        poller.register(self.pub_socket, zmq.POLLOUT)
        poller.poll()

        self._last_hb = 0
        self._local_subs = dict()

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
        publisher = {}
        publisher['socket'] = self.pub_socket

        address = [a for a in self.pub_socket_addrs
                   if a.startswith(('tcp', 'ipc'))][0]
        publisher['addr'] = address
        publisher['topic'] = topic
        self.publishers.append(publisher)
        self._broadcast.advertise(publisher['topic'], publisher['addr'])

        if topic not in self._local_subs:
            self._local_subs[topic] = []

        # Also connect to internal subscribers, if there are any
        for sub in self.subscribers:
            if sub['topic'] == topic:
                self._local_subs[topic].append(sub)

        if topic not in self._listeners:
            self._listeners[topic] = dict()

    def unadvertise(self, topic):
        """
        Unadvertise a topic.

        Parameters
        ----------
        topic : str
            Topic name.
        """
        self.publishers = [p for p in self.publishers if p['topic'] != topic]
        del self._listeners[topic]

    def subscribe(self, topic, cb):
        """
        Subscribe to the given topic.  Received messages will be passed to
        given the callback, which should have the signature: cb(msg).

        topic : str
            Name of topic.
        cb : callable
            Callable that accepts one argument (msg).
        """
        # Record what we're doing
        subscriber = {}
        subscriber['topic'] = topic
        subscriber['cb'] = cb
        self.subscribers.append(subscriber)
        self._broadcast.subscribe(subscriber['topic'], self.address)

        # Also connect to internal publishers, if there are any
        if topic in self._local_subs:
            self._local_subs[topic].append(subscriber)

    def unsubscribe(self, topic):
        """
        Unsubscribe from a given topic.

        Parameters
        ----------
        topic : str
            Name of topic.
        """
        self.subscribers = [s for s in self.subscribers if s['topic'] != topic]

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
        if [p for p in self.publishers if p['topic'] == topic]:
            packed_msg = pickle.dumps(msg, protocol=-1)
            self.pub_socket.send_multipart((topic.encode('utf-8'), packed_msg))
        for sub in self._local_subs[topic]:
            sub['cb'](msg)

    def _connect_subscriber(self, adv):
        """
        Internal method to connect to a publisher.
        """
        # Are we already connected to this publisher for this topic?
        if [c for c in self.sub_connections
                if c['topic'] == adv['topic'] and c['guid'] == adv['guid']]:
            return

        # Are we already connected to this publisher for this topic
        # on this address?
        for c in self.sub_connections:
            if c['topic'] == adv['topic'] and c['addr'] == adv['addr']:
                c['guid'] == adv['guid']
                return

        # Connect our subscriber socket
        conn = {}
        conn['socket'] = self.sub_socket
        conn['topic'] = adv['topic']
        conn['addr'] = adv['addr']
        conn['guid'] = adv['guid']
        conn['socket'].setsockopt(zmq.SUBSCRIBE, adv['topic'].encode('utf-8'))

        if not any([s['addr'] == adv['addr']
                    for s in self.sub_connections]):
            conn['socket'].connect(adv['addr'])

        self.sub_connections.append(conn)
        self.log.info('Connected to %s for %s (%s)' %
                      (adv['addr'], adv['topic'], adv['guid']))

    def get_listeners(self, topic):
        """
        Get a list of current listeners for a given topic.

        Parameters
        ----------
        topic : str
            Name of topic.
        """
        if topic not in self._listeners:
            return []
        return [addr for (addr, tstamp) in self._listeners[topic].items()
                if (time.time() - tstamp) < 2 * HB_REPEAT_PERIOD]

    def register_cb(self, cb, obj=None):
        """Register a callback to the spinOnce loop.

        Parameters
        ----------
        cb : callable
            Callback to register.
        obj : A zmq.Socket or any Python object having a ``fileno()``
            method that returns a valid file descriptor.  If not given,
            call when idle.
        """
        if not obj:
            self.idle_cbs.append(cb)
            return

        self.poller.register(obj, zmq.POLLIN)
        self.poll_cbs[obj] = cb

    def _handle_bcast_recv(self):

        data = self._broadcast.handle_recv()
        if not data:
            return

        topic = data['topic']
        addr = data['addr']

        if data['type'] == 'sub':
            # check for a new listener
            listeners = self._listeners
            if topic in listeners and addr not in listeners[topic]:
                [self._broadcast.advertise(p['topic'], p['addr'])
                 for p in self.publishers if p['topic'] == topic]

            # update our listeners
            if topic in listeners:
                listeners[topic][addr] = time.time()

        elif data['type'] == 'adv':
            # Are we interested in this topic?
            if [s for s in self.subscribers if s['topic'] == data['topic']]:
                # Yes, we're interested; make a connection
                self._connect_subscriber(data)

    def spinOnce(self, timeout=0.01):
        """
        Check for incoming messages, invoking callbacks.  If any items
        are found, spin again with a timeout of 0 to check for any new
        messages.

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
        items = dict(self.poller.poll(timeout))

        for (obj, poll_cb) in self.poll_cbs.items():
            if obj in items or obj.fileno() in items:
                poll_cb()

        if items.get(self._broadcast.sock.fileno(), None) == zmq.POLLIN:
            self._handle_bcast_recv()

        if items.get(self.sub_socket, None) == zmq.POLLIN:
            # Get the message (assuming that we get it all in one read)
            topic, msg = self.sub_socket.recv_multipart()
            topic = topic.decode('utf-8')

            msg = pickle.loads(msg)

            subs = [s for s in self.subscribers if s['topic'] == topic]
            if subs:
                if len(msg) == 1 and '___payload__' in msg:
                    msg = msg['___payload__']
                [s['cb'](msg) for s in subs]
                self.log.debug('Got message: ' + topic)

        if (time.time() - self._last_hb) > HB_REPEAT_PERIOD:
            self._last_hb = time.time()
            [self._broadcast.advertise(p['topic'], p['addr'])
             for p in self.publishers]
            [self._broadcast.subscribe(s['topic'], self.address)
             for s in self.subscribers]

        if items and timeout:
            self.spinOnce(timeout=0)  # avoid a context switch

        if not items or timeout:
            [cb() for cb in self.idle_cbs]

    def spin(self):
        """
        Give control to the message event loop.
        """
        while True:
            self.spinOnce(0.01)

    def close(self):
        """
        Close the DZMQ Interface and all of its ports.
        """
        self._broadcast.sock.close()
        self.pub_socket.close()
        self.sub_socket.close()

# Stolen from rosgraph
# https://github.com/ros/ros_comm/blob/hydro-devel/tools/rosgraph/src/rosgraph/network.py
# cache for performance reasons
_local_addrs = None


def get_local_addresses(use_ipv6=False, addrs=None, ifaces=None):
    """
    Get a list of local ip addresses that meet a given criteria.

    Parameters
    ----------
    use_ipv6 : bool, optional
        Whether to allow ipv6 addresses.
    addrs : list of strings, optional
        List of addresses to look for.
    ifaces : list strings, optional
        List of ethernet interfaces.

    Returns
    -------
    address : list of strings
        List of available local ip addresses that meet a given criteria.
    """
    # cache address data as it can be slow to calculate
    global _local_addrs
    if _local_addrs is not None:
        return _local_addrs

    ifaces = ifaces or netifaces.interfaces()

    v4addrs = []
    v6addrs = []
    for iface in ifaces:
        try:
            ifaddrs = netifaces.ifaddresses(iface)
        except ValueError:
            # even if interfaces() returns an interface name
            # ifaddresses() might raise a ValueError
            # https://bugs.launchpad.net/ubuntu/+source/netifaces/+bug/753009
            continue
        if socket.AF_INET in ifaddrs:
            v4addrs.extend([addr
                            for addr in ifaddrs[socket.AF_INET]
                            if 'broadcast' in addr])
        if socket.AF_INET6 in ifaddrs:
            v6addrs.extend([addr
                            for addr in ifaddrs[socket.AF_INET6]
                            if 'broadcast' in addr])
    if use_ipv6:
        local_addrs = v6addrs + v4addrs
    else:
        local_addrs = v4addrs
    if addrs:
        local_addrs = [a for a in local_addrs if not a['addr'] in addrs]
    _local_addrs = local_addrs
    return local_addrs
