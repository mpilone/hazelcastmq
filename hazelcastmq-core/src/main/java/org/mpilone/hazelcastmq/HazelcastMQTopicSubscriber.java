package org.mpilone.hazelcastmq;

import static java.lang.String.format;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.ITopic;

/**
 * A JMS topic subscriber for a HazecastMQ.
 * 
 * @author mpilone
 */
public class HazelcastMQTopicSubscriber extends HazelcastMQMessageConsumer
    implements TopicSubscriber {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * The Hazelcast listener used to monitor the topic.
   */
  private com.hazelcast.core.MessageListener<byte[]> hazelcastListener;

  /**
   * The in-memory queue used to buffer incoming messages for polling consumers.
   */
  private BlockingQueue<byte[]> memoryQueue;

  /**
   * The Hazelcast topic from which to consume.
   */
  private ITopic<byte[]> hazelcastTopic;

  /**
   * Constructs the subscriber on the given topic.
   * 
   * @param session
   *          the parent session
   * @param topic
   *          the topic from which to consume
   * @throws JMSException
   */
  public HazelcastMQTopicSubscriber(HazelcastMQSession session,
      HazelcastMQTopic topic) throws JMSException {
    super(session, topic);

    // If this is a topic, we have to immediately register a listener to get all
    // the messages.
    hazelcastListener = new com.hazelcast.core.MessageListener<byte[]>() {
      public void onMessage(com.hazelcast.core.Message<byte[]> hazelcastMsg) {
        onHazelcastTopicMessage(hazelcastMsg.getMessageObject());
      };
    };

    memoryQueue = new LinkedBlockingQueue<byte[]>(this.session.getConfig()
        .getTopicMaxMessageCount());
    hazelcastTopic = session.getHazelcast().getTopic(topic.getTopicName());
    hazelcastTopic.addMessageListener(hazelcastListener);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#close()
   */
  @Override
  public void close() throws JMSException {
    super.close();

    hazelcastTopic.removeMessageListener(hazelcastListener);
    memoryQueue.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TopicSubscriber#getNoLocal()
   */
  @Override
  public boolean getNoLocal() throws JMSException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.TopicSubscriber#getTopic()
   */
  @Override
  public Topic getTopic() throws JMSException {
    return (Topic) destination;
  }

  /**
   * Returns the topic name, ignoring any exceptions.
   * 
   * @return the topic name
   */
  private String safeTopicName() {
    try {
      return getTopic().getTopicName();
    }
    catch (JMSException ex) {
      // Ignore and return a default.
      return "unknown";
    }
  }

  /**
   * Called when a new message arrives from the Hazelcast topic. If a JMS
   * message listener is registered, the listener will be notified immediately.
   * If no listener is registered, the message will be buffered until the next
   * poll request.
   * 
   * @param msgData
   *          the raw message data
   */
  private void onHazelcastTopicMessage(byte[] msgData) {
    try {
      if (!started) {
        // If we are inactive, we ignore topic messages.
        return;
      }

      // We always queue the message even if we have a message listener. We'll
      // immediately pull it out of the queue and dispatch it if there is a
      // listener.
      if (!memoryQueue.offer(msgData)) {
        log.warn(format("In-memory message buffer full for topic [%s]. "
            + "Messages will be lost. Consider increaing the speed of "
            + "the consumer or the message buffer.", getTopic().getTopicName()));
        return;
      }

      // If we have a listener, drain any queued messages and push them to the
      // listener. In theory there should only be the very last message we
      // received in the queue to be consumed.
      if (messageListener != null) {
        boolean msgDispatched = false;
        do {
          msgDispatched = receiveAndDispatch(memoryQueue, messageListener);
        }
        while (msgDispatched);
      }
    }
    catch (Throwable ex) {
      log.error(
          format("Unable to buffer or deliver topic message for topic [%s].",
              safeTopicName()), ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.mpilone.hazelcastmq.HazelcastMQMessageConsumer#setMessageListener(javax
   * .jms.MessageListener)
   */
  @Override
  public void setMessageListener(MessageListener messageListener)
      throws JMSException {
    super.setMessageListener(messageListener);

    memoryQueue.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receive()
   */
  @Override
  public Message receive() throws JMSException {
    return receive(memoryQueue, -1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receive(long)
   */
  @Override
  public Message receive(long timeout) throws JMSException {
    return receive(memoryQueue, timeout);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.MessageConsumer#receiveNoWait()
   */
  @Override
  public Message receiveNoWait() throws JMSException {
    return receive(memoryQueue, 0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.HazelcastMQMessageConsumer#stop()
   */
  @Override
  void stop() {
    super.stop();

    memoryQueue.clear();
  }
}
