package org.mpilone.hazelcastmq.example;

/**
 *
 * @author mpilone
 */
public abstract class ExampleApp {

  public void runExample() {
    System.setProperty("hazelcast.logging.type", "slf4j");
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.org.mpilone.hazelcastmq",
        "info");
    System.setProperty("org.slf4j.simpleLogger.log.org.mpilone.yeti",
        "info");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "warn");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "warn");
    System.setProperty("org.slf4j.simpleLogger.log.org.apache.camel", "warn");

    try {
      start();

      if (isInputRequireBeforeStop()) {
        System.out.println("Press any key to continue...");
        System.in.read();
      }

      stop();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  protected abstract void start() throws Exception;

  protected void stop() throws Exception {
    // no op
  }

  protected boolean isInputRequireBeforeStop() {
    return false;
  }
}
