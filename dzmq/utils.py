import logging
import base64
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
