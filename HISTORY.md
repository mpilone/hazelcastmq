# HazelcastMQ History

## SNAPSHOT, v1.1.0

- [hazelcastmq-camel] Direct implementation of a polling consumer.
- [hazelcastmq-camel] Added support for per message destination selection via a 
message header.
- [yeti] Improved the performance and error handling of the StompFrameDecoder.
- [yeti] Implemented maximum frame size in the StompFrameDecoder.
- [yeti] Fixed a bug in the frame encoder where a 4 byte integer was written 
for the content-length rather than a String number.
- [yeti] Added slf4j as a dependency to support better logging and debugging.
- [yeti] Removed flag for enabling frame debugging from the StompClient and 
StompServer. It is now enabled by default and can be control via a logger 
configuration.
- [hazelcastmq-core] Added debug logging for sent and received messages.
- [yeti] Fixed a bug where EOL characters immediately after a frame cause a 
frame decoder exception because the proper index was not used in the incoming 
channel buffer. This should allow the Yeti client to be used with ActiveMQ 
STOMP.
- [hazelcastmq-stomp] Enabled STOMP 1.1 protocol support.

## 2014-06-20, v1.0.0

- Initial public release.