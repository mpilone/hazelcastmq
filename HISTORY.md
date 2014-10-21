# HazelcastMQ History

## SNAPSHOT, v1.4.0

- [hazelcastmq-core] Added a configuration property to support multiple
context dispatch strategies to allow for low thread usage or low latency.

## 2014-10-20, v1.3.0

- [hazelcastmq-core] Reworked the threading/locking in the context and core
to make it simpler and more consistent with the JMS 2 specification.
- [hazelcastmq-core] Exposed a XAHazelcastMQContext to allow access to an
XAResource for participation in global, two-phase commit transactions. (alpha)
- [hazelcastmq-core] Upgraded to Hazelcast 3.3.1.

## 2014-07-30, v1.2.0

- [hazelcastmq-examples] Upgraded to Spring 3.2.10
- [hazelcastmq-spring] New component to support better integration with Spring.
- [hazelcastmq-spring] Prototype of a Hazelcast/HazelcastMQ transaction manager and proxy.
- [hazelcastmq-camel, hazelcastmq-core] Bug fixes in the configuration classes 
  and additional convenience constructors.
- [hazelcastmq-core] Upgraded to Hazelcast 3.2.4.
- [hazelcastmq-camel] Relaxed the Camel endpoint destination type parsing to support leading colons and forward-slash separators.
- [hazelcastmq-core] Removed dependency on slf4j as Hazelcast's logging abstraction and configuration is now used.
- [hazelcastmq-example] Added a Camel async request/reply example.

## 2014-07-17, v1.1.0

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
- [hazelcastmq-core] Upgraded to Hazelcast 3.2.3.

## 2014-06-20, v1.0.0

- Initial public release.