# Copyright (C) 2013 Open Source Robotics Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import zmq
import uuid
import os
import logging
import platform
import struct
import threading
import signal
import sys
import netifaces
import base64
import json
try:
    from bson import Binary, BSON
except ImportError:
    BSON = None

try:
    import numpy as np
except ImportError:
    np = None

from .utils import get_log


# Defaults and overrides
ADV_SUB_PORT = 11312
MULTICAST_GRP = '224.1.1.1'
DZMQ_PORT_KEY = 'DZMQ_BCAST_PORT'
DZMQ_HOST_KEY = 'DZMQ_BCAST_HOST'
DZMQ_IP_KEY = 'DZMQ_IP'
DZMQ_IFACE_KEY = 'DZMQ_IFACE'

# Constants
OP_ADV = 0x01
OP_SUB = 0x02
UDP_MAX_SIZE = 512
GUID_LENGTH = 16
ADV_REPEAT_PERIOD = 1.0
VERSION = 0x0001
TOPIC_MAXLENGTH = 192
FLAGS_LENGTH = 16
ADDRESS_MAXLENGTH = 267
DEBUG = False


def unpack_msg(data):
    def unpack(obj):
        if not BSON:
            obj = json.loads(obj)
        for (key, value) in obj.items():
            if isinstance(value, dict):
                if ('shape' in value and 'dtype' in value and 'data' in value
                        and np):
                    if BSON is None:
                        value['data'] = base64.b64decode(value['data'])
                        obj[key] = np.fromstring(value['data'],
                                                 dtype=value['dtype'])
                    else:
                        obj[key] = np.frombuffer(value['data'],
                                                 dtype=value['dtype'])
                    obj[key] = obj[key].reshape(value['shape'])
                else:
                    # Make sure to recurse into sub-dicts
                    obj[key] = unpack(value)
        return obj

    if BSON is None:
        return unpack(data)
    else:
        return unpack(BSON(data).decode())


def pack_msg(obj):
    if not isinstance(obj, dict):
        obj = dict(___payload__=obj)
    for (key, value) in obj.items():
        if np and isinstance(value, np.ndarray):
            if BSON is None:
                data = base64.b64encode(value.tobytes())
            else:
                data = Binary(value.tobytes())
            obj[key] = dict(shape=value.shape,
                            dtype=value.dtype.str,
                            data=data)
        elif isinstance(value, dict):  # Make sure we recurse into sub-dicts
            obj[key] = pack_msg(value)
    if BSON is None:
        return json.dumps(obj)
    else:
        return BSON.encode(obj)


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

    def __init__(self, context=None, log=None, tcp_port=None, ipaddr=None):
        self.context = context or zmq.Context.instance()
        self.log = log or get_log()
        self.guid = uuid.uuid4()
        self.closed = False

        if DEBUG:
            self.log.setLevel(logging.DEBUG)

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
            self.bcast_host = addrs[0]['broadcast']
        else:
            if sys.platform == 'win32':
                self.ipaddr = '127.0.0.1'
                self.bcast_host = '255.255.255.255'
            elif 'linux' in sys.platform:
                self.ipaddr = '127.0.0.255'
                self.bcast_host = '127.0.0.255'
            else:
                self.ipaddr = '127.0.0.1'
                self.bcast_host = MULTICAST_GRP

        if ipaddr:
            self.ipaddr = ipaddr

        # What's our broadcast port?
        if DZMQ_PORT_KEY in os.environ:
            self.bcast_port = int(os.environ[DZMQ_PORT_KEY])
        else:
            # Take the default
            self.bcast_port = ADV_SUB_PORT
        # What's our broadcast host?
        if DZMQ_HOST_KEY in os.environ:
            self.bcast_host = os.environ[DZMQ_HOST_KEY]
        else:
            pass

        # Set up to listen to broadcasts
        self.bcast_recv = socket.socket(socket.AF_INET,  # Internet
                                        socket.SOCK_DGRAM,  # UDP
                                        socket.IPPROTO_UDP)
        self.bcast_recv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if platform.system() in ['Darwin']:
            self.bcast_recv.setsockopt(socket.SOL_SOCKET,
                                       socket.SO_REUSEPORT, 1)

        try:
            self._start_bcast_recv()
        except Exception:
            self.log.error("Could not open (%s, %s)" %
                           (self.bcast_host, self.bcast_port))
            raise

        # Set up to send broadcasts
        self.bcast_send = socket.socket(socket.AF_INET,  # Internet
                                        socket.SOCK_DGRAM,  # UDP
                                        socket.IPPROTO_UDP)
        self.bcast_send.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        if self.bcast_host == MULTICAST_GRP:
            self.bcast_send.setsockopt(socket.IPPROTO_IP,
                                       socket.IP_MULTICAST_TTL, 2)

        # Bookkeeping (which should be cleaned up)
        self.publishers = []
        self.subscribers = []
        self.sub_connections = []
        self.poller = zmq.Poller()
        # TODO: figure out what happens with multiple classes
        self.adv_timer = None
        self._advertisement_repeater()
        signal.signal(signal.SIGINT, self._sighandler)

        # Set up the one pub and one sub socket that we'll use
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket_addrs = []
        tcp_addr = 'tcp://%s' % (self.ipaddr)
        if not tcp_port:
            tcp_port = self.pub_socket.bind_to_random_port(tcp_addr)
            tcp_addr += ':%d' % (tcp_port)
        else:
            tcp_addr += ':%d' % tcp_port
            self.pub_socket.bind(tcp_addr)

        if len(tcp_addr) > ADDRESS_MAXLENGTH:
            raise Exception('TCP address length %d exceeds maximum %d'
                            % (len(tcp_addr), ADDRESS_MAXLENGTH))
        self.pub_socket_addrs.append(tcp_addr)
        self.pub_socket.setsockopt(zmq.LINGER, 0)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt(zmq.LINGER, 0)
        self.sub_socket_addrs = []

        self.poller.register(self.bcast_recv, zmq.POLLIN)
        self.poller.register(self.sub_socket, zmq.POLLIN)

    def _start_bcast_recv(self):
        if self.bcast_host == MULTICAST_GRP:
            self.bcast_recv.bind(('', self.bcast_port))
            mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GRP),
                               socket.INADDR_ANY)
            self.log.debug(self.bcast_port)
            self.log.debug(mreq)
            self.bcast_recv.setsockopt(socket.IPPROTO_IP,
                                       socket.IP_ADD_MEMBERSHIP,
                                       mreq)
        else:
            self.bcast_recv.bind((self.bcast_host, self.bcast_port))
            self.log.info("Opened (%s, %s)" %
                          (self.bcast_host, self.bcast_port))

    def _sighandler(self, sig, frame):
        self.adv_timer.cancel()
        sys.exit(0)

    def _advertise(self, publisher):
        """
        Internal method to pack and broadcast ADV message.
        """
        msg = b''
        msg += struct.pack('<H', VERSION)
        # TODO: pack the GUID more efficiently; should only need one call to
        # struct.pack()
        for i in range(0, GUID_LENGTH):
            msg += struct.pack('<B', (self.guid.int >> i * 8) & 0xFF)
        msg += struct.pack('<B', len(publisher['topic']))
        msg += publisher['topic'].encode('utf-8')
        msg += struct.pack('<B', OP_ADV)
        # Flags unused for now
        flags = [0x00] * FLAGS_LENGTH
        msg += struct.pack('<%dB' % (FLAGS_LENGTH), *flags)
        # We'll announce once for each address
        for addr in publisher['addresses']:
            if addr.startswith('inproc'):
                # Don't broadcast inproc addresses
                continue
            # Struct objects copy by value
            mymsg = msg
            mymsg += struct.pack('<H', len(addr))
            mymsg += addr.encode('utf-8')
            self.bcast_send.sendto(mymsg, (self.bcast_host, self.bcast_port))

    def advertise(self, topic):
        """
        Advertise the given topic.  Do this before calling publish().
        """
        if len(topic) > TOPIC_MAXLENGTH:
            raise Exception('Topic length %d exceeds maximum %d'
                            % (len(topic), TOPIC_MAXLENGTH))
        publisher = {}
        publisher['socket'] = self.pub_socket
        inproc_addr = 'inproc://%s' % topic
        # TODO: consider race condition in this test and set:
        if inproc_addr not in self.pub_socket_addrs:
            publisher['socket'].bind(inproc_addr)
            self.pub_socket_addrs.append(inproc_addr)
        tcp_addr = [a for a in self.pub_socket_addrs if a.startswith('tcp')][0]
        publisher['addresses'] = [inproc_addr, tcp_addr]
        publisher['topic'] = topic
        self.publishers.append(publisher)
        self._advertise(publisher)

        # Also connect to internal subscribers, if there are any
        adv = {}
        adv['topic'] = topic
        adv['address'] = inproc_addr
        adv['guid'] = self.guid
        for sub in self.subscribers:
            if sub['topic'] == topic:
                self._connect_subscriber(adv)

    def unadvertise(self, topic):
        self.publishers = [p for p in self.publishers if p['topic'] != topic]

    def _subscribe(self, subscriber):
        """
        Internal method to pack and broadcast SUB message.
        """
        msg = b''
        msg += struct.pack('<H', VERSION)
        # TODO: pack the GUID more efficiently; should only need one call to
        # struct.pack()
        for i in range(0, GUID_LENGTH):
            msg += struct.pack('<B', (self.guid.int >> i * 8) & 0xFF)
        msg += struct.pack('<B', len(subscriber['topic']))
        msg += subscriber['topic'].encode('utf-8')
        msg += struct.pack('<B', OP_SUB)
        # Flags unused for now
        flags = [0x00] * FLAGS_LENGTH
        msg += struct.pack('<%dB' % FLAGS_LENGTH, *flags)
        # Null body
        self.bcast_send.sendto(msg, (self.bcast_host, self.bcast_port))

    def subscribe(self, topic, cb, raw=False):
        """
        Subscribe to the given topic.  Received messages will be passed to
        given the callback, which should have the signature: cb(topic, msg).
        """
        # Record what we're doing
        subscriber = {}
        subscriber['topic'] = topic
        subscriber['cb'] = cb
        subscriber['raw'] = raw
        self.subscribers.append(subscriber)
        self._subscribe(subscriber)

        # Also connect to internal publishers, if there are any
        adv = {}
        adv['topic'] = subscriber['topic']
        adv['address'] = 'inproc://%s' % subscriber['topic']
        adv['guid'] = self.guid
        for pub in self.publishers:
            if pub['topic'] == subscriber['topic']:
                self._connect_subscriber(adv)

    def unsubscribe(self, topic):
        self.subscribers = [s for s in self.subscribers if s['topic'] != topic]

    def publish(self, topic, msg):
        """
        Publish the given message on the given topic.  You should have called
        advertise() on the topic first.
        """
        if [p for p in self.publishers if p['topic'] == topic]:
            msg = pack_msg(msg)
            self.pub_socket.send_multipart((topic.encode('utf-8'), msg))

    def _handle_adv_sub(self, msg):
        """
        Internal method to handle receipt of SUB and ADV messages.
        """
        try:
            data, addr = msg
            # Unpack the header
            offset = 0
            version = struct.unpack_from('<H', data, offset)[0]
            if version != VERSION:
                self.log.warn('Warning: mismatched protocol versions: %d != %d'
                              % (version, VERSION))
            offset += 2
            guid_int = 0
            for i in range(0, GUID_LENGTH):
                guid_int += struct.unpack_from('<B', data, offset)[0] << 8 * i
                offset += 1
            guid = uuid.UUID(int=guid_int)
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
                adv['guid'] = guid
                adv['flags'] = flags
                addresslength = struct.unpack_from('<H', data, offset)[0]
                offset += 2
                addr = data[offset:offset + addresslength]
                adv['address'] = addr.decode('utf-8')
                offset += addresslength

                # Are we interested in this topic?
                if [s for s in self.subscribers if s['topic'] == adv['topic']]:
                    # Yes, we're interested; make a connection
                    self._connect_subscriber(adv)

            elif op == OP_SUB:
                # The SUB body is NULL
                # If we're publishing this topic, re-advertise it to allow the
                # new subscriber to find us.
                [self._advertise(p) for p in self.publishers
                 if p['topic'] == topic]

            else:
                self.log.warn('Warning: got unrecognized OP: %d' % op)

        except Exception as e:
            self.log.exception(e)
            self.log.warn('Warning: exception while processing SUB or ADV '
                          'message: %s' % e)

    def _connect_subscriber(self, adv):
        """
        Internal method to connect to a publisher.
        """
        # Choose the best address to use.  If the publisher's GUID is the same
        # as our GUID, then we must both be in the same process, in which case
        # we'd like to use an 'inproc://' address.  Otherwise, fall back on
        # 'tcp://'.
        if adv['address'].startswith('tcp'):
            if adv['guid'] == self.guid:
                # Us; skip it
                return
        elif adv['address'].startswith('inproc'):
            if adv['guid'] != self.guid:
                # Not us; skip it
                return
        else:
            self.log.warn('Warning: ingoring unknown address type: %s' %
                          (adv['address']))
            return

        # Are we already connected to this publisher for this topic?
        if [c for c in self.sub_connections
                if c['topic'] == adv['topic'] and c['guid'] == adv['guid']]:
            return

        # Are we already connected to this publisher for this topic 
        # on this address?
        for c in self.sub_connections:
            if c['topic'] == adv['topic'] and c['address'] == adv['address']:
                c['guid'] == adv['guid']
                return

        # Connect our subscriber socket
        conn = {}
        conn['socket'] = self.sub_socket
        conn['topic'] = adv['topic']
        conn['address'] = adv['address']
        conn['guid'] = adv['guid']
        conn['socket'].setsockopt(zmq.SUBSCRIBE, adv['topic'].encode('utf-8'))

        if not any([s['address'] == adv['address']
                    for s in self.sub_connections]):
            conn['socket'].connect(adv['address'])

        self.sub_connections.append(conn)
        self.log.info('Connected to %s for %s (%s != %s)' %
                      (adv['address'], adv['topic'], adv['guid'], self.guid))

    def _advertisement_repeater(self):
        if self.closed:
            return
        [self._advertise(p) for p in self.publishers]
        self.adv_timer = threading.Timer(
            ADV_REPEAT_PERIOD, self._advertisement_repeater)
        self.adv_timer.start()

    def spinOnce(self, timeout=-1):
        """
        Check once for incoming messages, invoking callbacks for received
        messages.  Wait for up to timeout seconds.  For no waiting, set
        timeout=0. To wait forever, set timeout=-1.
        """
        if timeout < 0:
            # zmq interprets timeout=None as infinite
            timeout = None
        else:
            # zmq wants the timeout in milliseconds
            timeout = int(timeout * 1e3)

        # Look for sockets that are ready to read
        items = dict(self.poller.poll(timeout))

        if items.get(self.bcast_recv.fileno(), None) == zmq.POLLIN:
            self._handle_adv_sub(self.bcast_recv.recvfrom(UDP_MAX_SIZE))

        if items.get(self.sub_socket, None) == zmq.POLLIN:
            # Get the message (assuming that we get it all in one read)
            topic, msg = self.sub_socket.recv_multipart()
            topic = topic.decode('utf-8')

            raw_subs = [s for s in self.subscribers
                        if s['topic'] == topic and s['raw']]
            if raw_subs:
                [s['cb'](msg) for s in raw_subs]

            other_subs = [s for s in self.subscribers
                          if s['topic'] == topic and not s['raw']]
            if other_subs:
                msg = unpack_msg(msg)
                if len(msg) == 1 and '___payload__' in msg:
                    msg = msg['___payload__']
                [s['cb'](msg) for s in other_subs]

            if raw_subs or other_subs:
                self.log.debug('Got message: %s' % topic)

    def spin(self):
        """
        Give control to the message event loop.
        """
        while True:
            self.spinOnce(0.01)

    def close(self):
        self.closed = True
        self.bcast_recv.close()
        self.bcast_send.close()
        self.pub_socket.close()
        self.sub_socket.close()

# Stolen from rosgraph
# https://github.com/ros/ros_comm/blob/hydro-devel/tools/rosgraph/src/rosgraph/network.py
# cache for performance reasons
_local_addrs = None


def get_local_addresses(use_ipv6=False, addrs=None, ifaces=None):
    """
    :returns: known local addresses. Not affected by ROS_IP/ROS_HOSTNAME,
``[str]``
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
