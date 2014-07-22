# HazelcastMQ Spring

Provides integration with the [Spring Framework](http://projects.spring.io/spring-framework/) 
supporting Spring's transaction management.

## Features

* PlatformTransactionManager implementation for direct Hazelcast transaction management
* Transaction aware proxy for synchronization to transactions managed by other PlatformTransactionManagers

## Transaction Management

TODO

## Examples

The transaction support for HazelcastMQ Spring is configured just like any 
other Spring bean through either the Spring Java config or XML 
support.

### HazelcastTransactionManager

The HazelcastTransactionManager manages the Hazelcast transaction as the main application transaction. The implementation integrates with Spring's Transactional method annotations to provide annotation driven transaction management when combined with HazelcastUtils to locate transactional instances of distributed objects.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/spring/tx/TransactionManager.java).

### TransactionAwareHazelcastProxyFactory

The TransactionAwareHazelcastProxyFactory creates a proxy for a HazelcastInstance to automatically join an existing main transaction (Hazelcast or other) and hide any transactional behavior from the caller.

View the [example](../hazelcastmq-examples/src/main/java/org/mpilone/hazelcastmq/example/spring/tx/TransactionProxy.java).