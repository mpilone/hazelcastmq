package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.*;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of producing to a queue within a transaction and attempting to
 * consume from a different node.
 *
 * @author mpilone
 */
public class TransactionalSend extends ExampleApp {

  private final ILogger log = Logger.getLogger(TransactionalSend.class);

  public static void main(String[] args) {
    TransactionalSend app = new TransactionalSend();
    app.runExample();
  }

  @Override
  protected void start() throws Exception {

    final DataStructureKey key = new DataStructureKey("transactional.example",
        QueueService.SERVICE_NAME);

    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    try {
      BrokerConfig brokerConfig = new BrokerConfig(hz);
      try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {
        ChannelContext channelContext1 = broker.createChannelContext();
        channelContext1.setAutoCommit(false);
        Channel channel1 = channelContext1.createChannel(key);

        ChannelContext channelContext2 = broker.createChannelContext();
        Channel channel2 = channelContext2.createChannel(key);

        log.info("Sending message on channel 1.");
        channel1.send(new GenericMessage<>("Sent to channel1"));

        // See if we can read the messages before commit.
        log.info("Reading the message before commit.");
        org.mpilone.hazelcastmq.core.Message<?> msg = channel2.receive(1,
            TimeUnit.SECONDS);
        Assert.isNull(msg, "Expected null. Something went wrong!");

        log.info("Committing the transaction on channel 1.");
        channelContext1.commit();

        log.info("Reading the message after commit.");
        msg = channel2.receive(1, TimeUnit.SECONDS);
        log.info("Got message payload: " + msg.getPayload());
      }
    }
    finally {
      hz.shutdown();
    }
  }
}
