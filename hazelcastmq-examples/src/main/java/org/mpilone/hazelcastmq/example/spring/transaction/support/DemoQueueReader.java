package org.mpilone.hazelcastmq.example.spring.transaction.support;

import java.util.concurrent.Executors;

import org.mpilone.hazelcastmq.core.*;
import org.slf4j.*;

/**
 * A single thread that attempts to read from a queue channel until shutdown.
 *
 * @author mpilone
 */
public class DemoQueueReader {

  private static final Logger log =
      LoggerFactory.getLogger(DemoQueueReader.class);

  private final DataStructureKey channelKey = DataStructureKey.fromString(
      "/queue/demo.queue");
  private final SingleThreadPollingMessageDispatcher dispatcher;

  public DemoQueueReader(Broker broker) {

    dispatcher = new SingleThreadPollingMessageDispatcher();
    dispatcher.setBroker(broker);
    dispatcher.setChannelKey(channelKey);
    dispatcher.setExecutor(Executors.newCachedThreadPool());
    dispatcher.setPerformer(msg -> {
      log.info("Read data: {}", msg.getPayload());
    });
    dispatcher.start();
  }

  public void shutdown() {
    dispatcher.stop();
    dispatcher.getExecutor().shutdown();
  }

}
