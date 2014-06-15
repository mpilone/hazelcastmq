# HazelcastMQ JMS

A JMS 1.1 implementation which layers on top of HazelcastMQ Core. While 
not a full implementation of the specification, connections, sessions, producers, and 
consumers on queues and topics are implemented. HazelcastMQ JMS can be used with 
the [Spring Framework's](http://projects.spring.io/spring-framework/) JmsTemplate or
[Apache Camel's](http://camel.apache.org/) JMS Component to provide a drop-in replacement
for existing brokers.

## Features
* JMS 1.1 APIs implemented
* Send and receive from queues, topics, and temporary queues
* Transactional message sending (per thread, not session)
* Text and Bytes message types
* Message expiration (in the consumer only)
* Connection start/stop suspended consumer delivery

## Not Implemented Yet
* Object or Stream message types
* Persistence selection per message
* Queue or topic browsing
* Probably 100 other things I've missed

## Not Going to Work Any Time Soon
* Transactional message reception
* Message selectors
* Durable subscriptions
* Message priority

## Examples

Using HazelcastMQ JMS is a layer on the Core functionality and is similar to 
using any JMS 1.1 provider:

1. Create a connection factory
2. Create a connection
3. Create a session
4. Create a message producer or consumer
5. Send or receive messages

### Spring JmsTemplate
Of course you can skip a lot of the JMS API by using support libraries such as Spring's 
JMSTemplate.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/jms/SpringJmsTemplateOneWay.java).