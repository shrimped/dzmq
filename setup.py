try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Simple library to do discovery on top of zeromq messaging',
    'author': 'Steven Silvester',
    'url': 'https://github.com/shrimped/disc_zmq',
    'author_email': 'steven.silvester@ieee.org',
    'version': '0.1',
    'install_requires': ['pyzmq', 'netifaces'],
    'packages': ['dzmq'],
    'name': 'disc_zmq'
}

setup(**config)
