package org.mpilone.hazelcastmq.example.core;

import static java.lang.String.format;

import java.util.concurrent.*;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of subscribing to a queue and sending a large number of messages to
 * the queue. This example uses the multiple thread dispatch strategy so each of
 * the 3 consumer contexts will get a thread.
 */
public class QueueNameLargeSendAsync extends ExampleApp implements
    MessageDispatcher.Performer {

  private final static ILogger log = Logger.getLogger(
      QueueNameLargeSendAsync.class);

  private final static int MESSAGE_COUNT = 30000;

  private final CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);

  @Override
  public void perform(org.mpilone.hazelcastmq.core.Message<?> msg) {
    receiveLatch.countDown();
  }

  public static void main(String[] args) throws Exception {
    QueueNameLargeSendAsync app = new QueueNameLargeSendAsync();
    app.runExample();
  }

  /**
   * Constructs the example.
   *
   * @throws Exception if the example fails
   */
  @Override
  public void start() throws Exception {

    final DataStructureKey destination = DataStructureKey.fromString(
        "/queue/example.dest");

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcastMQ broker.
    BrokerConfig brokerConfig = new BrokerConfig(hz);
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      MultipleThreadPollingMessageDispatcher dispatcher1 =
          new MultipleThreadPollingMessageDispatcher();
      dispatcher1.setBroker(broker);
      dispatcher1.setChannelKey(destination);
      dispatcher1.setPerformer(this);
      dispatcher1.setMaxConcurrentPerformers(3);
      dispatcher1.setExecutor(executor);
      dispatcher1.start();

      try (ChannelContext mqContext = broker.createChannelContext();
          Channel channel = mqContext.createChannel(destination)) {

        log.info("Sending messages.");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < MESSAGE_COUNT; ++i) {
          org.mpilone.hazelcastmq.core.Message<String> msg = MessageBuilder.
              withPayload("Hello World!").build();
          channel.send(msg);
        }

        log.info("Receiving all messages.");
        receiveLatch.await(10, TimeUnit.SECONDS);

        long endTime = System.currentTimeMillis();
        long elapsed = endTime - startTime;
        long msgsPerSec = (long) (MESSAGE_COUNT * (1000.0 / elapsed));

        log.info(format("Sent and received %d messages in %d milliseconds "
            + "(in parallel) for an average of %d messages per second.",
            MESSAGE_COUNT, elapsed, msgsPerSec
        ));

        dispatcher1.stop();
      }
    }
    finally {
      hz.shutdown();
      executor.shutdown();
    }
  }
}
