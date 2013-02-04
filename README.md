# HazelcastMQ

HazelcastMQ is a JMS Provider for [Hazelcast](http://www.hazelcast.com/), an in-memory 
data grid. HazelcastMQ emphasizes simplicity over performance and currently does not implement
the entire JMS specification. However the basics are there and it can be used with 
the [Spring Framework's](http://www.springsource.org/spring-framework) JmsTemplate or
[Apache Camel's](http://camel.apache.org/) JMS Component.

HazelcastMQ also contains HazelcastMQ Stomper which is a STOMP 
[stomp](http://stomp.github.com) server which maps all SEND and SUBSCRIBE commands to JMS
producers and consumers. This allows non-Java components (such as C, C++, Python, Ruby, etc.)
to interact with the MQ capabilities of HazelcastMQ. While Stomper was specifically written
for HazelcastMQ as the JMS provider, it should work with any JMS provider. Stomper is not
required when using the JMS facilities but may be used to support a wider range of messaging 
endpoints.

## Rationale

Refer to my [initial blog post](http://mikepilone.blogspot.com/2013/01/hazelcast-jms-provider.html) for now.

## JMS Features (in hazelcastmq-core)

### Implemented
* JMS 1.1 APIs implemented
* Send and receive from queues, topics, and temporary queues
* Transactional message sending (per thread, not session)
* Text and Bytes message types
* Message expiration (in the consumer only)
* Connection start/stop suspended consumer delivery

### Not Implemented Yet
* Object or Stream message types
* Persistence selection per message
* Queue or topic browsing
* Probably 100 other things I've missed

### Not Going to Work Any Time Soon
* Transactional message reception
* Message selectors
* Durable subscriptions
* Message priority

## STOMP Server Features (in hazelcastmq-stomper)

### Implemented
* STOMP 1.2 protocol (which is mostly backward compatible to 1.1)
* Sending and subscribing
* Multiple clients on single server
* Queue and Topic send/receive

### Not Implemented Yet
* Transactions (BEGIN, COMMIT, ABORT)
* Probably 100 other things I've missed
* Header encoding/decoding of special characters
* Heart-beat
* Protocol version negotiation

## Not Going to Work Any Time Soon
* Transaction message reception or ACK/NACK (i.e. always auto ACK)

## Examples

Using HazelcastMQ is similar to using any JMS provider:

1. Create a connection factory
2. Create a connection
3. Create a session
4. Create a message producer or consumer
5. Send or receive messages

Using Stomper is a simple layer on the core JMS functionality:

1. Create a connection factory
2. Create a stomper configuration
3. Create a stomper
4. Connect with STOMP clients

### Simple Request and Reply
This example shows a simple request and reply message pattern where both the requester 
and replier are implemented in the same code.

View the [example](https://github.com/mpilone/hazelcastmq/blob/master/hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/ProducerConsumerRequestReply.java).

### Spring JmsTemplate
Of course you can skip a lot of the JMS API by using support libraries such as Spring's 
JMSTemplate.

View the [example](https://github.com/mpilone/hazelcastmq/blob/master/hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/SpringJmsTemplateOneWay.java).

### Node Failure
One of the major benefits of using Hazelcast as the message transport/store is that it 
offers flexible reliability and replication options. This example shows a (local) three 
node cluster and how messages can be produced and consumed even in the event of a single 
or multiple node failure in the cluster. If you've ever worked with a clustered JMS broker 
before, you'll appreciate the simplicity of this configuration.

View the [example](https://github.com/mpilone/hazelcastmq/blob/master/hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/NodeFailure.java).

## Future Work

If there is interest I plan on continuing development of the provider to support more 
of the JMS API and feature set. Let me know what you think and what features you need most.
