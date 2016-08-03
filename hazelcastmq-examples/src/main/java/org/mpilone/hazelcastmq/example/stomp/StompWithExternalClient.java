package org.mpilone.hazelcastmq.example.stomp;

import java.io.File;

import javax.jms.ConnectionFactory;

import org.mpilone.hazelcastmq.core.HazelcastMQ;
import org.mpilone.hazelcastmq.core.HazelcastMQConfig;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.stomp.HazelcastMQStomp;
import org.mpilone.hazelcastmq.stomp.StompAdapterConfig;
import org.mpilone.hazelcastmq.stomp.StompAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * This example uses a STOMP server to accept connections from any 3rd party
 * STOMP client. The Stomper server is backed by the HazelcastMQ
 * {@link ConnectionFactory} which is backed by a local Hazelcast instance. The
 * server can be shutdown by deleting the anchor file created at startup.
 * 
 * @author mpilone
 * 
 */
public class StompWithExternalClient {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
    System.setProperty("org.slf4j.simpleLogger.log.com.hazelcast", "info");
    System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

    new StompWithExternalClient();
  }

  public StompWithExternalClient() throws Exception {

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
      // Create the HazelcaseMQ instance.
      HazelcastMQConfig mqConfig = new HazelcastMQConfig();
      mqConfig.setHazelcastInstance(hazelcast);
      HazelcastMQInstance mqInstance = HazelcastMQ
          .newHazelcastMQInstance(mqConfig);

      // Create a Stomp server.
      StompAdapterConfig stompConfig = new StompAdapterConfig(
          mqInstance);
      StompAdapter stompServer = HazelcastMQStomp.newStompAdapter(stompConfig);

      log.info("Stomp server is now listening on port: "
          + stompConfig.getPort());

      log.info("Remove file " + anchorFile.getName() + " to shutdown.");

      while (anchorFile.exists()) {
        Thread.sleep(1000);
      }

      log.info("Shutting down Stomper.");
      stompServer.shutdown();
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }

  }

}
