# HazelcastMQ History

## SNAPSHOT, v1.1.0

- [hazelcast-camel] Direct implementation of a polling consumer.
- [hazelcast-camel] Added support for per message destination selection via a 
message header.
- [yeti] Improved the performance and error handling of the StompFrameDecoder.
- [yeti] Implemented maximum frame size in the StompFrameDecoder.
- [yeti] Fixed a bug in the frame encoder where a 4 byte integer was written 
for the content-length rather than a String number.
- [yeti] Added slf4j as a dependency to support better logging and debugging.
- [yeti] Removed flag for enabling frame debugging from the StompClient and 
StompServer. It is now enabled by default and can be control via a logger 
configuration.
- [hazelcast-core] Added debug logging for sent and received messages.

## 2014-06-20, v1.0.0

- Initial public release.