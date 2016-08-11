package org.mpilone.hazelcastmq.example.stomp;

import java.io.File;

import org.mpilone.hazelcastmq.core.*;
import org.mpilone.hazelcastmq.example.ExampleApp;
import org.mpilone.hazelcastmq.stomp.HazelcastMQStomp;
import org.mpilone.hazelcastmq.stomp.StompAdapterConfig;
import org.mpilone.hazelcastmq.stomp.StompAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * This example runs a HazelcastMQ STOMP adapter to accept connections from any
 * 3rd party STOMP client. The server can be shutdown by deleting the anchor
 * file created at startup.
  * 
 * @author mpilone
 * 
 */
public class StompWithExternalClient extends ExampleApp {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  public static void main(String[] args) throws Exception {
    new StompWithExternalClient().runExample();
  }

  @Override
  public void start() throws Exception {

    // Create a shutdown anchor to shut things down cleanly.
    File anchorFile = new File("stomper_example_anchor.lck");
    anchorFile.delete();
    anchorFile.createNewFile();
    anchorFile.deleteOnExit();

    // Create a Hazelcast instance.
    Config config = new Config();
    HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

    // Create the HazelcaseMQ instance.
    BrokerConfig brokerConfig = new BrokerConfig(hazelcast);

    try (Broker broker = HazelcastMQ.newBroker(brokerConfig)) {

      // Create a Stomp server.
      StompAdapterConfig stompConfig = new StompAdapterConfig(broker);
      try (StompAdapter stompServer = HazelcastMQStomp.newStompAdapter(
          stompConfig)) {

      log.info("Stomp server is now listening on port: "
          + stompConfig.getPort());

      log.info("Remove file " + anchorFile.getName() + " to shutdown.");

      while (anchorFile.exists()) {
        Thread.sleep(1000);
      }

      log.info("Shutting down Stomper.");
      }
    }
    finally {
      hazelcast.getLifecycleService().shutdown();
    }

  }

}
