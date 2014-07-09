package org.mpilone.hazelcastmq.stomp.server;

import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.yeti.StompFrameDecoder;

/**
 * The configuration of the STOMP server.
  * 
 * @author mpilone
 */
public class HazelcastMQStompConfig {

  private int maxFrameSize;
  private int port;
  private HazelcastMQInstance hazelcastMQInstance;
  private FrameConverter frameConverter;

  /**
   * Constructs a configuration which will not have an MQ instance set. A MQ
   * instance must be set before constructing a {@link HazelcastMQStomp}
   * instance.
   */
  public HazelcastMQStompConfig() {
    this(null);
  }

  /**
   * Constructs a configuration which will have a number of defaults:
   * <ul>
   * <li>port: 8032</li>
   * <li>frameConverter: {@link DefaultFrameConverter}</li>
   * <li>frameDebugEnabled: false</li>
   * <li>maxFrameSize: {@link StompFrameDecoder#DEFAULT_MAX_FRAME_SIZE}</li>
   * </ul>
   * 
   * @param mqInstance the HzMq instance to use for all message consumers and
     *          producers
   */
  public HazelcastMQStompConfig(HazelcastMQInstance mqInstance) {
    this.hazelcastMQInstance = mqInstance;

    frameConverter = new DefaultFrameConverter();
    port = 8032;
    maxFrameSize = StompFrameDecoder.DEFAULT_MAX_FRAME_SIZE;
  }

  /**
   * Sets the maximum supported frame size in bytes. Frames larger than this
   * size will be considered invalid and an invalid frame will be generated.
   *
   * @param maxFrameSize the maximum frame size in bytes
   */
  public void setMaxFrameSize(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
  }

  /**
   * Returns the maximum supported frame size in bytes. Frames larger than this
   * size will be considered invalid and an invalid frame will be generated.
   *
   * @return the maximum frame size in bytes
   */
  public int getMaxFrameSize() {
    return maxFrameSize;
  }

  /**
   * Sets the port to which the server will bind to listen for incoming
   * connections.
   *
   * @param port the port number
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the port to which the server will bind to listen for incoming
   * connections.
   *
   * @return the port number
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the MQ instance to use for all message consumers and   * producers.
   * 
   * @return the MQ instance
   */
  public HazelcastMQInstance getHazelcastMQInstance() {
    return hazelcastMQInstance;
  }

  /**
   * Sets the MQ instance to use for all message consumers and producers.
   *
   * @param mqInstance the MQ instance
   */
  public void setHazelcastMQInstance(HazelcastMQInstance mqInstance) {
    this.hazelcastMQInstance = mqInstance;
  }

  /**
   * The frame converter used to convert STOMP frames into HazelcastMQ messages.
    * 
   * @return the frame converter
   */
  public FrameConverter getFrameConverter() {
    return frameConverter;
  }

  /**
   * Sets the the frame converter used to convert STOMP frames into HazelcastMQ
   * messages.
   *
   * @param frameConverter
   *          the frameConverter to set
   */
  public void setFrameConverter(FrameConverter frameConverter) {
    this.frameConverter = frameConverter;
  }
}
