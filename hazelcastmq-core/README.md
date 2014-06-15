# HazelcastMQ Core

The core MQ library that provides a JMS 2.0-like API for sending
and receiving messages. Core has no dependencies on JMS and can be used directly 
as a light weight messaging framework. Other messaging APIs can be layered on 
top of the core API to provide compatibility with existing frameworks and tools.

## Features
* Send and receive from queues, topics, and temporary queues
* Transactional message sending (per thread, not context due to Hazelcast limitation)
* Message expiration (in the consumer only)
* Request/reply pattern using correlation IDs and reply-to destinations

## Not Going to Work Any Time Soon
* Transactional message reception

## Examples

A code example of sending a message using HazelcastMQ Core is shown below. Normally
the MQ instance is created at application startup using your DI framework of choice.

    HazelcastMQConfig mqConfig = new HazelcastMQConfig();
    mqConfig.setHazelcastInstance(hz);

    HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig); // (1)
    HazelcastMQContext mqContext = mqInstance.createContext(); // (2)

    HazelcastMQMessage msg = new HazelcastMQMessage();
    msg.setContentAsString("Hello World!");
    
    HazelcastMQProducer mqProducer = mqContext.createProducer(); // (3)
    mqProducer.send("/queue/example.dest", msg); // (4)
    
    mqContext.close();
    mqInstance.shutdown();

Using HazelcastMQ Core is similar to using the JMS 2.0 API (but with no 
JMS dependencies):

1. Create a HazelcastMQ instance
2. Create a HazelcastMQ context
3. Create a message producer or consumer
4. Send or receive messages

### Simple Send and Receive
This example shows a simple send and receive message pattern where both the producer 
and consumer are implemented in the same code.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/core/SimpleProducerConsumer.java).

### Node Failure
One of the major benefits of using Hazelcast as the message transport/store is that it 
offers flexible reliability and replication options. This example shows a (local) three 
node cluster and how messages can be produced and consumed even in the event of a single 
or multiple node failure in the cluster. If you've ever worked with a clustered JMS broker 
before, you'll appreciate the simplicity of this configuration.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/core/NodeFailure.java).
