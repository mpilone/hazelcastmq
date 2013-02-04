package org.mpilone.hazelcastmq.stomper;

import java.io.Closeable;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;

import javax.jms.*;

/**
 * IO utility methods for streams and JMS.
 * 
 * @author mpilone
 */
class IoUtil {

  /**
   * The UTF-8 character set used for all conversions.
   */
  final static Charset UTF_8 = Charset.forName("UTF-8");

  /**
   * Closes the given instance, ignoring any exceptions.
   * 
   * @param closeable
   */
  public static void safeClose(Closeable closeable) {
    try {
      closeable.close();
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
  public static void safeClose(Socket socket) {
    try {
      socket.close();
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
  public static void safeClose(ServerSocket socket) {
    try {
      socket.close();
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
