# HazelcastMQ Camel

An [Apache Camel](http://camel.apache.org/) component implementation for 
HazelcastMQ supporting Camel's integration framework and Enterprise Integration 
Patterns (EIP). The component supports configurable consumers and producers 
including request/reply messaging and concurrent consumers. Unlike Camel's 
existing JMS component, HazelcastMQ Camel has no dependency on the Spring 
Framework by building directly on HazelcastMQ Core.

## Features
* Configurable endpoint with producers and consumers
* Request/reply messaging pattern via temporary or exclusive reply queues
* Concurrent consumers
* No dependencies beyond HazelcastMQ Core

## Not Implemented Yet
* Transactions
* More advanced integration with Camel's asynchronous patterns

## Endpoint Format

The endpoint URI can specify if the destination is a queue or a topic such 
as `hazelcastmq:queue:my.outgoing.orders` or 
`hazelcastmq:topic:order.processed.event`. The destination will be transformed 
into a standard HzMq destination such as `/queue/my.outgoing.orders` or 
`/topic/order.processed.event`. If queue or topic is not specified, a queue 
will be assumed.

URI parameters can be used to customize the behavior of the endpoint. The 
parameters are specified using normal URI parameter syntax such as:

```
hazelcastmq:queue:my.outgoing.orders?timeToLive=30000&requestTimeout=10000
```

## Endpoint Parameters

Endpoint parameters are specified as URI parameters when defining the endpoint
and can be used to customize the behavior of the endpoint for all messages 
going in or out.

Parameter | Type | Default | Description
--------- | ---- | ------- | -----------
concurrentConsumers | int | 1 | The number of concurrent consumers to use when consuming from an endpoint. This should always be set to 1 for topics or duplicate messages may be received.
requestTimeout | int | 20000 | The number of milliseconds to wait for a reply when performing request/reply messaging.
timeToLive | int | 0 | The number of milliseconds that a message will live after being produced by an endpoint. A value of 0 indicates that the message never expires.
replyTo | String | null | The reply-to destination for all replies to messages produced by this endpoint. If not set and a request/reply message is sent, a unique temporary reply queue will be created. The reply queue should be exclusive to avoid having reply messages consumed by other endpoints.

## Custom Message Headers

Message headers can be used to customize the behavior of an endpoint on a 
message by message basis.

Header | Type | Description
------ | -----| -----------
CamelHzMqDestination | String | If specified, the destination name will be used rather than the destination configured in the endpoint. This allows for a single endpoint to produce messages to multiple destinations by setting different values for this header.


## Examples

A code example of using HazelcastMQ Camel is shown below. Normally
the Camel context, including the HazelcastMQ component is created at 
application startup using your DI framework of choice.

    HazelcastMQConfig mqConfig = new HazelcastMQConfig();
    mqConfig.setHazelcastInstance(hz);

    HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig); 
    
    HazelcastMQCamelConfig mqCamelConfig = new HazelcastMQCamelConfig(); // (1)
    mqCamelConfig.setHazelcastMQInstance(mqInstance);

    HazelcastMQCamelComponent mqCamelComponent =
        new HazelcastMQCamelComponent(); // (2)
    mqCamelComponent.setConfiguration(mqCamelConfig);

    // Create the Camel context. This could be done via a Spring XML file.
    CamelContext camelContext = new DefaultCamelContext();
    camelContext.addComponent("hazelcastmq", mqCamelComponent); // (3)

    camelContext.addRoutes(new RouteBuilder() { // (4)
      @Override
      public void configure() {
        from("hazelcastmq:queue:primo.test?timeToLive=10000")
            .to("hazelcastmq:queue:secondo.test");
      }
    });

    camelContext.start();

HazelcastMQ Camel is configured like any other Camel component:

1. Create a component configuration
2. Create the HazelcastMQ Camel component
3. Register the component with the Camel context
4. Configure endpoints in routes
5. Send exchanges to or receive exchanges from the endpoint

### Consume and Producer Route
A simple route can be setup to consume messages from one HazelcastMQ endpoint and produces them to another.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/camel/CamelToCamelOneWay.java).