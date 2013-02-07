package org.mpilone.hazelcastmq.stompee;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.mpilone.hazelcastmq.stomp.Frame;

public class HazelcastMQStompeeConfig {
  private String address;
  private int port;
  private BlockingQueue<Frame> frameFetchQueue;

  /**
   * Constructs a configuration which will have a number of defaults:
   * <ul>
   * <li>port: 8032</li>
   * <li>address: localhost</li>
   * <li>executor: {@link ArrayBlockingQueue} of size 5</li>
   * </ul>
   */
  public HazelcastMQStompeeConfig() {
    this("localhost", 8032);
  }

  /**
   * Constructs a configuration which will use the given address and port to
   * reach the STOMP server and the following defaults:
   * 
   * <ul>
   * <li>executor: {@link ArrayBlockingQueue} of size 5</li>
   * </ul>
   * 
   * @param address
   *          the STOMP server address (hostname or IP)
   * @param port
   *          the STOMP server port
   */
  public HazelcastMQStompeeConfig(String address, int port) {
    this.address = address;
    this.port = port;
    this.frameFetchQueue = new ArrayBlockingQueue<Frame>(5);
  }

  /**
   * @return the frameFetchQueue
   */
  public BlockingQueue<Frame> getFrameFetchQueue() {
    return frameFetchQueue;
  }

  /**
   * @param frameFetchQueue
   *          the frameFetchQueue to set
   */
  public void setFrameFetchQueue(BlockingQueue<Frame> frameFetchQueue) {
    this.frameFetchQueue = frameFetchQueue;
  }

  /**
   * @return the address
   */
  public String getAddress() {
    return address;
  }

  /**
   * @param address
   *          the address to set
   */
  public void setAddress(String address) {
    this.address = address;
  }

  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }

  /**
   * @param port
   *          the port to set
   */
  public void setPort(int port) {
    this.port = port;
  }
}
