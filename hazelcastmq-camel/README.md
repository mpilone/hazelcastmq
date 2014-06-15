# HazelcastMQ Camel

An [Apache Camel](http://camel.apache.org/) component implementation for 
HazelcastMQ supporting Camel's integration framework and Enterprise Integration 
Patterns (EIP). The component supports configurable consumers and producers 
including request/reply messaging and concurrent consumers. Unlike Camel's 
existing JMS component, HazelcastMQ Camel has not dependency on the Spring 
Framework by building directly on HazelcastMQ Core.

## Features
* Configurable endpoint with producers and consumers
* Request/reply messaging pattern via temporary or exclusive reply queues
* Concurrent consumers
* No dependencies beyond HazelcastMQ Core

## Not Implemented Yet
* Transactions
* More advanced integration with Camel's asynchronous and polling patterns

## Not Going to Work Any Time Soon

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
        from("hazelcastmq:queue:primo.test")
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