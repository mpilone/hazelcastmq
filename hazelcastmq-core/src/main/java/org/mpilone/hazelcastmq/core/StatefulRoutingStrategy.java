package org.mpilone.hazelcastmq.core;

/**
 * A {@link RoutingStrategy} that maintains state when routing messages. A
 * stateful strategy will be persisted after each call to route messages so the
 * state is maintained between executions of the associated router. Stateful
 * strategies are powerful, but they may be a little slower than stateless
 * strategies as they must be loaded and persisted for each routing operation.
 *
 * @author mpilone
 */
public interface StatefulRoutingStrategy extends RoutingStrategy {

}
