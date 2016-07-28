package org.mpilone.hazelcastmq.example.core;

import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.Assert;
import org.mpilone.hazelcastmq.example.ExampleApp;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Example of producing to a queue and attempting to consume from a different
 * node in a transaction.
 * 
 * @author mpilone
 */
public class TransactionalReceive extends ExampleApp {

  private final static ILogger log = Logger.
      getLogger(TransactionalReceive.class);

  public static void main(String[] args) {
    TransactionalReceive app = new TransactionalReceive();
    app.runExample();
  }

  @Override
  public void start() throws Exception {
    final DataStructureKey key = new DataStructureKey("transactional.example",
        QueueService.SERVICE_NAME);

    Config config = new Config();
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    try {
      BrokerConfig brokerConfig = new BrokerConfig(hz);
      try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {
        ChannelContext channelContext1 = broker.createChannelContext();
        Channel channel1 = channelContext1.createChannel(key);

        ChannelContext channelContext2 = broker.createChannelContext();
        channelContext2.setAutoCommit(false);
        Channel channel2 = channelContext2.createChannel(key);

        log.info("Sending message on channel 1.");
        channel1.send(new GenericMessage<>("Sent to channel1"));

        log.info("Reading the message in transaction on channel 2.");
        org.mpilone.hazelcastmq.core.Message<?> msg = channel2.receive(1,
            TimeUnit.SECONDS);
        log.info("Got message payload: " + msg.getPayload());

        log.info("Reading the message on channel 1.");
        msg = channel1.receive(1, TimeUnit.SECONDS);
        Assert.isNull(msg, "Got a duplicate message on channel 1.");

        log.info("Rolling back the transaction on channel 2.");
        channelContext2.rollback();

        log.info("Reading the message on channel 1.");
        msg = channel1.receive(1, TimeUnit.SECONDS);
        log.info("Got message payload: " + msg.getPayload());
      }
    }
    finally {
      hz.shutdown();
    }
  }
}
