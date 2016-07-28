package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of using the read-ready listener to be notified when a channel may
 * have a message.
 */
public class QueueChannelSendReceiveReadReady extends ExampleApp {

  private final static ILogger log = Logger.getLogger(
      QueueChannelSendReceiveReadReady.class);

  private final DataStructureKey key = new DataStructureKey("async.test",
      QueueService.SERVICE_NAME);


  public static void main(String[] args) throws Exception {
    QueueChannelSendReceiveReadReady app =
        new QueueChannelSendReceiveReadReady();
    app.runExample();
  }

  /**
   * Constructs the example.
   * 
   * @throws Exception if the example fails
   */
  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    try {
      BrokerConfig brokerConfig = new BrokerConfig(hz);
      try (final Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

        Worker worker = new Worker(key, broker);
        Thread thread = new Thread(worker);
        thread.start();

        ChannelContext context = broker.createChannelContext();
        Channel channel = context.createChannel(key);

        for (int i = 0; i < 10; ++i) {
          channel.send(new GenericMessage<>("Data set " + i));
        }

        // Wait for all the messages to be consumed.
        Thread.sleep(2000);

        worker.shutdown();
        thread.join();
      }
    }
    finally {
      hz.shutdown();
    }
  }

  /**
   * A worker that uses a {@link ReadReadyListener} to process messages when it
   * is notified that they have arrived.
   */
  private static class Worker implements Runnable {

    private final Broker broker;
    private final DataStructureKey key;
    private final ReadReadySelector selector;
    private volatile boolean shutdown;

    public Worker(DataStructureKey key, Broker broker) {
      this.broker = broker;
      this.key = key;
      this.selector = new ReadReadySelector();
    }

    void shutdown() {
      shutdown = true;
      selector.wakeup();
    }

    @Override
    public void run() {
      try (ChannelContext context = broker.createChannelContext()) {

        // Create two channels to demonstrate the read/ready listener.
        Channel channel1 = context.createChannel(key);
        Channel channel2 = context.createChannel(key);

        channel1.addReadReadyListener(selector);
        channel2.addReadReadyListener(selector);

        while (!shutdown) {
          selector.select(1, TimeUnit.MINUTES).stream().forEach(c -> {
            org.mpilone.hazelcastmq.core.Message<?> msg = c.receive(0,
                TimeUnit.SECONDS);

            if (msg != null) {
              log.info("Got message: " + msg.getPayload());
            }
          });
        }
      }
    }
  }
}
