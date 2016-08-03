# HazelcastMQ STOMP Server

A [STOMP](http://stomp.github.com) 
server which maps all SEND and SUBSCRIBE commands to HazelcastMQ Core
channels. This allows non-Java components (such as C, C++, Python, Ruby, etc.)
to interact with the MQ capabilities of HazelcastMQ. HazelcastMQ STOMP may be 
used to integrate into existing frameworks such as [Apache Camel's](http://camel.apache.org/)
while not requiring any of the JMS complexity associated with existing brokers and STOMP endpoints.

## Features
* STOMP 1.1 and 1.2 protocols
* Sending and subscribing
* Multiple clients on single server
* Queue and Topic send/receive
* Header encoding/decoding of special characters
* Transactions (BEGIN, SEND, COMMIT, ABORT)

## Not Implemented Yet
* Acks (ACK, NACK)
* Transaction message reception or ACK/NACK (i.e. always auto ACK)

## Examples

    HazelcastMQConfig mqConfig = new HazelcastMQConfig();
    mqConfig.setHazelcastInstance(hazelcast);
    HazelcastMQInstance mqInstance = HazelcastMQ
        .newHazelcastMQInstance(mqConfig); // (1)

    HazelcastMQStompConfig stompConfig = new HazelcastMQStompConfig(
        mqInstance); // (2)

    HazelcastMQStompInstance stompServer = HazelcastMQStomp.
        newHazelcastMQStompInstance(stompConfig); // (3)

    log.info("Stomp server is now listening on port: "
        + stompConfig.getPort());

Using HazelcastMQ STOMP requires configuring a simple layer on the Core functionality:

1. Create a HazelcastMQ broker
2. Create a stomp configuration
3. Create a stomp server
4. Connect with STOMP clients

### STOMP Send and STOMP Receive
Using the Stomp Server on top of HazelcastMQ Core allows Hazelcast to be 
used as a distributed messaging system for components written in any language. The STOMP 
protocol is lightweight and simple to understand.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/stomp/StompToStompOneWay.java).

### STOMP Send and JMS Receive
Using the Stomp Server on top of HazelcastMQ Core allows a STOMP message 
to be sent from a STOMP client and then consumed by a JMS consumer as a JMS Message. 
This allows non-Java components to produce and consumer messages while allowing Java 
components to use the rich JMS API and enterprise integration patterns.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/stomp/StompToJmsOneWay.java).