package org.mpilone.hazelcastmq.stompee;


public class HazelcastMQStompeeConfig {
  private String address;

  private int port;

  /**
   * The maximum number of frames that will be queued for consumption when a
   * frame listener is not in use. If the incoming frame queue exceeds this
   * value, frames will be dropped.
   */
  private int frameMaxQueueCount;

  /**
   * Constructs a configuration which will have a number of defaults:
   * <ul>
   * <li>port: 8032</li>
   * <li>address: localhost</li>
   * <li>frameMaxQueueCount: 10</li>
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
   * <li>frameMaxQueueCount: 10</li>
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
    this.frameMaxQueueCount = 10;
  }

  /**
   * 
   * @return
   */
  public int getFrameMaxQueueCount() {
    return frameMaxQueueCount;
  }

  /**
   * 
   * @param frameMaxQueueCount
   */
  public void setFrameMaxQueueCount(int frameMaxQueueCount) {
    this.frameMaxQueueCount = frameMaxQueueCount;
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
