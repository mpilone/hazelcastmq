package org.mpilone.hazelcastmq.stomper;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;

import javax.jms.*;

class IoUtil {

  public static String stringFromBytes(byte[] data, String charset) {
    try {
      return new String(data, charset);
    }
    catch (UnsupportedEncodingException ex) {
      throw new StompException("Unable to create String from bytes.", ex);
    }
  }

  public static byte[] stringToBytes(String data, String charset) {
    try {
      return data.getBytes(charset);
    }
    catch (UnsupportedEncodingException ex) {
      throw new StompException("Unable to create bytes from String.", ex);
    }
  }

  public static void safeClose(Closeable closeable) {
    try {
      closeable.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  public static void safeClose(Socket socket) {
    try {
      socket.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  public static void safeClose(ServerSocket socket) {
    try {
      socket.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  public static void safeClose(Connection connection) {
    try {
      connection.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  public static void safeClose(Session session) {
    try {
      session.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  public static void safeClose(MessageProducer producer) {
    try {
      producer.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

  public static void safeClose(MessageConsumer consumer) {
    try {
      consumer.close();
    }
    catch (Throwable ex) {
      // Ignore
    }
  }

}
