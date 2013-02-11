package org.mpilone.hazelcastmq.example;

/**
 * Simple assertions for the examples.
 * 
 * @author mpilone
 */
public class Assert {
  public static void notNull(Object obj, String msg) {
    if (obj == null) {
      throw new RuntimeException(msg);
    }
  }

  public static void isNull(Object obj, String msg) {
    if (obj != null) {
      throw new RuntimeException(msg);
    }
  }

  public static void isTrue(boolean value, String msg) {
    if (!value) {
      throw new RuntimeException(msg);
    }
  }
}
