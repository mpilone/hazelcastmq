package org.mpilone.hazelcastmq.example.spring.tx.support.hzmq;

import org.mpilone.hazelcastmq.core.*;
import org.slf4j.*;

/**
 * A single thread that attempts to read from a queue until shutdown.
 *
 * @author mpilone
 */
public class DemoQueueReader {

  private static final Logger log =
      LoggerFactory.getLogger(DemoQueueReader.class);
  private final HazelcastMQContext mqContext;
  private final HazelcastMQConsumer mqConsumer;

  public DemoQueueReader(HazelcastMQInstance hazelcastMQInstance) {

    mqContext = hazelcastMQInstance.createContext();

    mqConsumer = mqContext.createConsumer("/queue/demo.queue");
    mqConsumer.setMessageListener(new HazelcastMQMessageListener() {
      @Override
      public void onMessage(HazelcastMQMessage msg) {
        log.info("Read data: {}", msg.getBodyAsString());
      }
    });

  }

  public void shutdown() {
    mqConsumer.close();
    mqContext.close();
  }

}
