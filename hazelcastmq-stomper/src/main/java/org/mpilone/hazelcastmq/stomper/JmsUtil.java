package org.mpilone.hazelcastmq.stomper;

import javax.jms.*;

/**
 * IO utility methods for JMS sessions, producers, and consumers.
 * 
 * @author mpilone
 */
class JmsUtil {

  /**
   * Closes the given instance, ignoring any exceptions.
   * 
   * @param closeable
   */
  public static void safeClose(Connection connection) {
    try {
      connection.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  /**
   * Closes the given instance, ignoring any exceptions.
   * 
   * @param closeable
   */
  public static void safeClose(Session session) {
    try {
      session.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  /**
   * Closes the given instance, ignoring any exceptions.
   * 
   * @param closeable
   */
  public static void safeClose(MessageProducer producer) {
    try {
      producer.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  /**
   * Closes the given instance, ignoring any exceptions.
   * 
   * @param closeable
   */
  public static void safeClose(MessageConsumer consumer) {
    try {
      consumer.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

}
