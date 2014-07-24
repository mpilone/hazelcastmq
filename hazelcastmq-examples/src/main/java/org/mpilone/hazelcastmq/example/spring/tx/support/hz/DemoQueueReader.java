
package org.mpilone.hazelcastmq.example.spring.tx.support.hz;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

/**
 * A single thread that attempts to read from a queue until shutdown.
 *
 * @author mpilone
 */
public class DemoQueueReader implements Runnable {
  private volatile boolean shutdown = false;
  private final Thread t;
  private final HazelcastInstance hazelcastInstance;
  private static final Logger log =
      LoggerFactory.getLogger(DemoQueueReader.class);

  public DemoQueueReader(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
    t = new Thread(this);
    t.start();
  }

  @Override
  public void run() {
    IQueue<String> demoQueue = hazelcastInstance.getQueue("demo.queue");
    while (!shutdown) {
      String data = null;
      try {
        data = demoQueue.poll(2, TimeUnit.SECONDS);
      }
      catch (InterruptedException ex) {
        // ignore
      }
      if (data != null) {
        log.info("Read data: {}", data);
      }
    }
  }

  public void shutdown() {
    shutdown = true;
    try {
      t.join();
    }
    catch (InterruptedException ex) {
      // ignore
    }
  }

}
