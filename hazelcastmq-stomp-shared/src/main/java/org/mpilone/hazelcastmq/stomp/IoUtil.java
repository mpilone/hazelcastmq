package org.mpilone.hazelcastmq.stomp;

import java.io.Closeable;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;

/**
 * IO utility methods for streams and sockets.
 * 
 * @author mpilone
 */
public class IoUtil {

  /**
   * The UTF-8 character set used for all conversions.
   */
  public final static Charset UTF_8 = Charset.forName("UTF-8");

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

}
