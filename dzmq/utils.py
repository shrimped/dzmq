import logging
import base64
import socket

import netifaces
try:
    import ujson as json
except ImportError:
    import json

try:
    import numpy as np
except ImportError:
    np = None


def get_log(name=None):
    """Return a console logger.

    Output may be sent to the logger using the `debug`, `info`, `warning`,
    `error` and `critical` methods.

    Parameters
    ----------
    name : str
        Name of the log.

    References
    ----------
    .. [1] Logging facility for Python,
           http://docs.python.org/library/logging.html

    """
    if name is None:
        name = 'pybisonmq'
    else:
        name = 'pybisonmq.' + name

    log = logging.getLogger(name)
    log.setLevel(logging.WARN)
    return log


def _setup_log():
    """Configure root logger.

    """
    import logging
    import sys

    try:
        handler = logging.StreamHandler(stream=sys.stdout)
    except TypeError:  # pragma: no cover
        handler = logging.StreamHandler(strm=sys.stdout)

    log = get_log()
    log.addHandler(handler)


_setup_log()


def unpack_msg(data):
    """
    Unpack a binary data message into a dictionary.

    Parameters
    ----------
    data : bytes
        Binary data message.

    Returns
    -------
    out : dict
        Unpacked message.
    """
    def unpack(obj):
        obj = json.loads(obj.decode('utf-8'))
        for (key, value) in obj.items():
            if isinstance(value, dict):
                if ('shape' in value and 'dtype' in value and 'data' in value
                        and np):
                    value['data'] = base64.b64decode(value['data'])
                    obj[key] = np.fromstring(value['data'],
                                             dtype=value['dtype'])
                    obj[key] = obj[key].reshape(value['shape'])
                else:
                    # Make sure to recurse into sub-dicts
                    obj[key] = unpack(value)
        return obj

    return unpack(data)


def pack_msg(obj):
    """
    Pack an object into a binary data message.

    Parameters
    ----------
    obj : str or dictionary
        Object to pack.

    Returns
    -------
    out : bytes
        Binary data message.
    """
    if not isinstance(obj, dict):
        obj = dict(___payload__=obj)
    for (key, value) in obj.items():
        if np and isinstance(value, np.ndarray):
            data = base64.b64encode(value.tobytes()).decode('utf-8')
            obj[key] = dict(shape=value.shape,
                            dtype=value.dtype.str,
                            data=data)
        elif isinstance(value, dict):  # Make sure we recurse into sub-dicts
            obj[key] = pack_msg(value)

    return json.dumps(obj).encode('utf-8')


# Taken from rosgraph
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
