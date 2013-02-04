package org.mpilone.hazelcastmq.example;

import java.io.File;

import org.mpilone.hazelcastmq.HazelcastMQConfig;
import org.mpilone.hazelcastmq.HazelcastMQConnectionFactory;
import org.mpilone.hazelcastmq.stomper.HazelcastMQStomper;
import org.mpilone.hazelcastmq.stomper.HazelcastMQStomperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class StompWithStomper {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");

    new StompWithStomper();
  }

  public StompWithStomper() throws Exception {

    // Create a shutdown anchor to shut things down cleanly.
    File anchorFile = new File("stomper_example_anchor.lck");
    anchorFile.delete();
    anchorFile.createNewFile();
    anchorFile.deleteOnExit();

    // Create a Hazelcast instance.
    Config config = new Config();
    config.setProperty("hazelcast.logging.type", "slf4j");
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    try {
      // Setup the connection factory.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      HazelcastMQConnectionFactory connectionFactory = new HazelcastMQConnectionFactory(
          hazelcast, mqConfig);

      // Setup Stomper
      HazelcastMQStomperConfig stomperConfig = new HazelcastMQStomperConfig();
      stomperConfig.setConnectionFactory(connectionFactory);
      HazelcastMQStomper stomper = new HazelcastMQStomper(stomperConfig);

      log.info("Stomper is now listening on port: " + stomperConfig.getPort());
      log.info("Remove file " + anchorFile.getName() + " to shutdown.");

      while (anchorFile.exists()) {
        Thread.sleep(1000);
      }

      log.info("Shutting down Stomper.");
      stomper.shutdown();
      stomperConfig.getExecutor().shutdown();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }

  }

}
