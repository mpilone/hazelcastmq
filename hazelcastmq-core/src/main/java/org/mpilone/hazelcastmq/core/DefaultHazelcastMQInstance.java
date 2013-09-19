package org.mpilone.hazelcastmq.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DefaultHazelcastMQInstance implements HazelcastMQInstance {

  private HazelcastMQConfig config;

  private Map<String, HazelcastMQContext> contextMap;

  private TopicMessageRelayer topicRelayer;

  static final String TXN_TOPIC_QUEUE_NAME = "hazelcastmq.txn-topic";

  static final String TXN_TOPIC_QUEUE_DESTINATION = Headers.DESTINATION_QUEUE_PREFIX
      + TXN_TOPIC_QUEUE_NAME;

  public DefaultHazelcastMQInstance(HazelcastMQConfig config) {
    this.config = config;

    contextMap = new ConcurrentHashMap<String, HazelcastMQContext>();

    // Setup a subscription to the transactional topic queue.
    topicRelayer = new TopicMessageRelayer();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQInstance#shutdown()
   */
  @Override
  public void shutdown() {

    // Stop the topic relayer.
    topicRelayer.shutdown();

    // Stop all the contexts.
    List<HazelcastMQContext> contexts = new ArrayList<HazelcastMQContext>(
        contextMap.values());

    for (HazelcastMQContext context : contexts) {
      context.close();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQInstance#getConfig()
   */
  @Override
  public HazelcastMQConfig getConfig() {
    return config;
  }

  void onContextClosed(String id) {
    contextMap.remove(id);
  }

  private class TopicMessageRelayer implements HazelcastMQMessageListener {

    private HazelcastMQContext context;

    private HazelcastMQConsumer consumer;

    public TopicMessageRelayer() {
      context = createContext();

      consumer = context.createConsumer(TXN_TOPIC_QUEUE_DESTINATION);
      consumer.setMessageListener(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.mpilone.hazelcastmq.core.HazelcastMQMessageHandler#handle(org.mpilone
     * .hazelcastmq.core.HazelcastMQMessage)
     */
    @Override
    public void onMessage(HazelcastMQMessage msg) {
      context.createProducer().send(msg.getDestination(), msg);
    }

    public void shutdown() {
      consumer.close();
      context.close();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.HazelcastMQInstance#createContext()
   */
  @Override
  public HazelcastMQContext createContext() {
    return createContext(false);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.core.HazelcastMQInstance#createContext(boolean)
   */
  @Override
  public HazelcastMQContext createContext(boolean transacted) {
    DefaultHazelcastMQContext context = new DefaultHazelcastMQContext(
        transacted, this);

    contextMap.put(context.getId(), context);
    return context;
  }
}
