[![Coverage Status](https://coveralls.io/repos/shrimped/dzmq/badge.svg)](https://coveralls.io/r/shrimped/dzmq)

Simple library (with reference Python implementation) to do discovery
on top of zeromq messaging. This is a modification of the ROS 2.0 zmq
prototype to implement messaging + discovery via zmq with serialization
handled through binary pickling.  Also provides topic  synchronization capability through the `get_listeners` method.

Raw message definitions:

  * Header (HDR):
    * VERSION: 2 bytes
    * GUID: 16 bytes; ID that is unique to the process, generated according
      to RFC 4122
    * TOPICLENGTH: 1 byte; length, in bytes, of TOPIC
    * TOPIC: string; max 192 bytes
    * TYPE: 1 byte
    * FLAGS: 16 bytes (unused for now)

  * advertisement (ADV):
    * HDR (TYPE = 1)
    * ADDRESSLENGTH: 2 bytes; length, in bytes, of ADDRESS
    * ADDRESS: one valid ZeroMQ address (e.g., "tcp://10.0.0.1:6000")

  * subscription (SUB):
    * HDR (TYPE = 2)
    * ADDRESSLENGTH: 2 bytes; length, in bytes, of ADDRESS
    * ADDRESS: one valid ZeroMQ address (e.g., "tcp://10.0.0.1:6000")


ZeroMQ message definitions (for which we will let zeromq handle framing):

  * publication (PUB) multipart messages, with the following parts:
    * TOPIC (placed first to facilitate filtering)
    * BODY: opaque bytes


Defaults and conventions:

  * Default port for Broadcast messages: 11312
  * By convention, Raw messages are sent to a broadcast address.
  * In raw messages, all integers are sent little-endian


API sketch:

  * `(un)advertise(topic)`
  * `subscribe(topic, cb)`
    * `cb(msg)`
  * `unsubscribe(topic)`
  * `publish(topic, msg)`
  * `get_listeners(topic)`
