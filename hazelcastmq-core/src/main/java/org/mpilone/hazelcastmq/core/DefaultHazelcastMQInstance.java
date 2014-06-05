package org.mpilone.hazelcastmq.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the {@link HazelcastMQInstance}.
 *
 * @author mpilone
 */
class DefaultHazelcastMQInstance implements HazelcastMQInstance {

  /**
   * The instance configuration.
   */
  private final HazelcastMQConfig config;

  /**
   * The map of active contexts from ID to context instance.
   */
  private final Map<String, HazelcastMQContext> contextMap;

  /**
   * A consumer that is responsible for relaying messages from a transactional
   * queue to a topic.
   */
  private final TopicMessageRelayer topicRelayer;

  /**
   * The name of the transactional queue used for buffering transactional topic
   * messages.
   */
  static final String TXN_TOPIC_QUEUE_NAME = "hazelcastmq.txn-topic";

  /**
   * The full destination name of the transactional queue used for buffering
   * transactional topic messages.
   */
  static final String TXN_TOPIC_QUEUE_DESTINATION = Headers.DESTINATION_QUEUE_PREFIX
      + TXN_TOPIC_QUEUE_NAME;

  /**
   * Constructs the instance.
   *
   * @param config the instance configuration
   */
  public DefaultHazelcastMQInstance(HazelcastMQConfig config) {
    this.config = config;

    contextMap = new ConcurrentHashMap<>();

    // Setup a subscription to the transactional topic queue.
    topicRelayer = new TopicMessageRelayer();
  }

  @Override
  public void shutdown() {

    // Stop all the contexts. The list of contexts is duplicated because they
    // will report being closed and removed from the map during this operation.
    List<HazelcastMQContext> contexts = new ArrayList<>(contextMap.values());
    for (HazelcastMQContext context : contexts) {

      // Close the context unless it is the one being used by our internal
      // relayer. We'll do that last to make sure any pending topic messages
      // are properly relayed before shutdown.
      if (context != topicRelayer.getContext()) {
        context.close();
      }
    }

    // Stop the topic relayer.
    topicRelayer.shutdown();
  }

  @Override
  public HazelcastMQConfig getConfig() {
    return config;
  }

  /**
   * Called by a context when it is closed so it can be properly removed from
   * the list of active contexts.
   *
   * @param id the ID of the context
   */
  void onContextClosed(String id) {
    contextMap.remove(id);
  }

  /**
   * A consumer that is responsible for relaying messages from a transactional
   * queue to a topic. This is required because Hazelcast does not support
   * transactional topics and so HazelcastMQ fakes it by writing messages to a
   * transactional queue and then relaying the messages to the appropriate topic
   * when the messages are committed to the queue.
   */
  private class TopicMessageRelayer implements HazelcastMQMessageListener {

    /**
     * The context used by the relayer.
     */
    private final HazelcastMQContext context;

    /**
     * The consumer used by the relayer.
     */
    private final HazelcastMQConsumer consumer;

    /**
     * Constructs the relayer which will immediately create a context and begin
     * listening for messages.
     */
    public TopicMessageRelayer() {
      context = createContext();

      consumer = context.createConsumer(TXN_TOPIC_QUEUE_DESTINATION);
      consumer.setMessageListener(this);
    }

    @Override
    public void onMessage(HazelcastMQMessage msg) {
      context.createProducer().send(msg.getDestination(), msg);
    }

    /**
     * Shuts down the relayer by closing the internal consumer and context.
     */
    public void shutdown() {
      consumer.close();
      context.close();
    }

    /**
     * Returns the context used by this relayer.
     *
     * @return the context used by the relayer
     */
    public HazelcastMQContext getContext() {
      return context;
    }
  }

  @Override
  public HazelcastMQContext createContext() {
    return createContext(false);
  }

  @Override
  public HazelcastMQContext createContext(boolean transacted) {
    DefaultHazelcastMQContext context = new DefaultHazelcastMQContext(
        transacted, this);

    contextMap.put(context.getId(), context);
    return context;
  }
}
