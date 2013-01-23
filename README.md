# HazelcastMQ

HazelcastMQ is a JMS Provider for [Hazelcast](http://www.hazelcast.com/), an in-memory 
data grid. HazelcastMQ emphasizes simplicity over performance and currently does not implement
the entire JMS specification. However the basics are there and it can be used with 
the [Spring Framework's](http://www.springsource.org/spring-framework) JmsTemplate or
[Apache Camel's](http://camel.apache.org/) JMS Component.

## Rationale

Refer to my [initial blog post](http://mikepilone.blogspot.com/2013/01/hazelcast-jms-provider.html) for now.

## Features

### Implemented
* JMS 1.1 APIs implemented
* Send and receive from queues, topics, and temporary queues
* Transactional message sending
* Text message type

### Not Implemented Yet
* Message expiration
* Byte, Object or Stream message types
* Persistence selection per message
* Queue or topic browsing
* Connection start/stop behavior
* Probably 100 other things I've missed

### Not Going to Work Any Time Soon
* Transactional message receiving
* Message selectors
* Durable subscriptions

## Examples

Using HazelcastMQ is similar to using any JMS provider:

1. Create a connection factory
2. Create a connection
3. Create a session
4. Create a message producer or consumer
5. Send or receive messages

### Simple Request and Reply
This example shows a simple request and reply message pattern where both the requester 
and replier are implemented in the same code.

View the [example](https://github.com/mpilone/hazelcastmq/blob/master/hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/ProducerConsumerRequestReply.java).

### Spring JmsTemplate
Of course you can skip a lot of the JMS API by using support libraries such as Spring's 
JMSTemplate.

View the [example](https://github.com/mpilone/hazelcastmq/blob/master/hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/SpringJmsTemplateOneWay.java).

## Future Work

If there is interest I plan on continuing development of the provider to support more 
of the JMS API and feature set. Let me know what you think and what features you need most.
