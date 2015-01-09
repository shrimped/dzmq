try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'pybsonmq',
    'author': 'Donald Venable',
    'url': 'https://bitbucket.org/blink1073/aimq',
    'author_email': 'donald.venable@us.af.mil',
    'version': '0.1',
    'install_requires': ['nose', 'pyzmq', 'netifaces'],
    'packages': ['pybsonmq'],
    'scripts': [],
    'name': 'pybsonmq'
}

setup(**config)
