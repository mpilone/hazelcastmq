package org.mpilone.hazelcastmq.stomp;

import java.io.Closeable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * IO utility methods for streams and sockets.
 * 
 * @author mpilone
 */
public class IoUtil {

  /**
   * Attempts an await on the given latch and ignores exceptions.
   * 
   * @param latch
   *          the latch to wait on
   * @param timeout
   *          the timeout
   * @param unit
   *          the time unit
   * @return the value of await or false if there is an exception
   */
  public static boolean safeAwait(CountDownLatch latch, long timeout,
      TimeUnit unit) {
    try {
      return latch.await(timeout, unit);
    }
    catch (InterruptedException ex) {
      // Ignore
    }

    return false;
  }

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
