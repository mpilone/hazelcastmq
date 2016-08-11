package org.mpilone.hazelcastmq.example.core;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Example of sending a message to a queue and then waiting for a reply.
 */
public class QueueChannelRequestReply extends ExampleApp {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new QueueChannelRequestReply().runExample();
  }

  @Override
  public void start() throws Exception {

    // Create a Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcastMQ broker.
    BrokerConfig brokerConfig = new BrokerConfig(hz);
    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      final DataStructureKey replyDest = DataStructureKey.fromString(
          "/queue/example.dest.replies");
      final DataStructureKey requestDest = DataStructureKey.fromString(
          "/queue/example.dest");

      try (ChannelContext mqContext = broker.createChannelContext()) {

        // Create the request and replu channels.
        try (Channel requestChannel = mqContext.createChannel(requestDest);
            Channel replyChannel = mqContext.createChannel(replyDest)) {

          Message<String> msg = MessageBuilder.withPayload(
              "Do some work and get back to me.").setHeader(
                  MessageHeaders.REPLY_TO, replyDest).
              setHeader(MessageHeaders.CORRELATION_ID, UUID.randomUUID().
                  toString()).build();

          replyChannel.markTemporary();

          // Send a message with a reply-to header.
          requestChannel.send(msg);

          msg = (Message<String>) requestChannel.receive(2, TimeUnit.SECONDS);
          DataStructureKey rd = msg.getHeaders().getReplyTo();
          String corrId = msg.getHeaders().getCorrelationId();

          log.info("Got message [{}]. Sending reply to [{}].", msg.
              getPayload(), rd);

          // Build the reply message.
          msg = MessageBuilder.withPayload("Work is done. You're welcome.").
              setHeader(MessageHeaders.CORRELATION_ID, corrId).build();
          replyChannel.send(msg);

          // Read the reply.
          msg = (Message<String>) replyChannel.receive(2, TimeUnit.SECONDS);

          log.info("Got reply message [{}].", msg.getPayload());
        }
      }
    }
    finally {
      hz.shutdown();
    }
  }
}
