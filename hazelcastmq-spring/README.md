# HazelcastMQ Spring

Provides integration with the [Spring Framework](http://projects.spring.io/spring-framework/) supporting Spring's transaction management and synchronization. This component is considered BETA and the API may change as various transactional use cases are identified and tested.

## Features

* PlatformTransactionManager implementation for direct Hazelcast/HazelcastMQ transaction management
* Transaction aware proxy for synchronizing Hazelcast/HazelcastMQ transactions to Spring managed transactions with other PlatformTransactionManager implementations

## Transaction Management

Spring provides a [transaction management](http://docs.spring.io/spring/docs/4.0.6.RELEASE/spring-framework-reference/htmlsingle/#transaction) framework that offers a number of ways to integrate while abstracting away the differences of the underlying container transaction implementation (if any). An understanding of Spring's core features is recommended.

HazelcastMQ Spring provides transaction integration by using either a HazelcastMQ specific transaction manager or by synchronizing with an existing transaction managed by a non-HazelcastMQ transaction manager (such as JDBC or JTA). Like other Spring transaction integrations, a HazelcastMQUtils class should be used to access a HazelcastMQContext that is linked to the local transaction (if any). For code that cannot be modified to use the HazelcastMQUtils, a TransactionAwareHazelcastMQProxyFactory is provided which creates a proxy for a HazelcastMQInstance that internally uses HazelcastMQUtils to create HazelcastMQContexts.

## Examples

The transaction support for HazelcastMQ Spring is configured just like any 
other Spring bean through either the Spring Java config or XML 
support.

### HazelcastMQTransactionManager

The HazelcastMQTransactionManager manages the HazelcastMQ transaction (and therefore the Hazelcast transaction) as the main application transaction. The implementation integrates with Spring's @Transactional method annotations to provide annotation driven transaction management when combined with HazelcastMQUtils to locate transactional instances of distributed objects.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/spring/tx/HzMqTransactionManager.java).

### TransactionAwareHazelcastMQProxyFactory

The TransactionAwareHazelcastMQProxyFactory creates a proxy for a HazelcastMQInstance to automatically join an existing main transaction (HazelcastMQ or other) and hide any transactional behavior from the caller.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/spring/tx/HzMqTransactionProxy.java).
