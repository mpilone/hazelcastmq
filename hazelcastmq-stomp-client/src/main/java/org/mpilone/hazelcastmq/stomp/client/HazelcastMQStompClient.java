package org.mpilone.hazelcastmq.stomp.client;

import static org.mpilone.hazelcastmq.stomp.IoUtil.safeAwait;
import static org.mpilone.hazelcastmq.stomp.IoUtil.safeClose;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.*;

import org.mpilone.hazelcastmq.stomp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A STOMP client that can be used with any STOMP server; however it was
 * designed and tested against HazelcastMQ Stomper. To use the client, create a
 * {@link HazelcastMQStompClientConfig} and pass it to
 * {@link HazelcastMQStompClient#HazelcastMQStompClient(org.mpilone.hazelcastmq.stomp.client.HazelcastMQStompClientConfig) }.
 * The client will immediately connect to the server and perform the CONNECT
 * handshake. Frames can then be sent with the {@link #send(Frame)} method and
 * received by pull via {@link #receive(long, TimeUnit)} or push via
 * {@link #setFrameListener(FrameListener)}.
 * </p>
 * <p>
 * When using the pull mechanism, the client maintains an internal queue of
 * frames. If this queue is full, new frames will be dropped. Therefore the
 * queue size must be configured appropriately based on the consumption speed or
 * slow processing should be quickly handed off to another thread.
 * </p>
 * <p>
 * The {@link FrameBuilder} class can be used to construct common frames for
 * sending.
 * </p>
 * 
 * @author mpilone
 */
public class HazelcastMQStompClient {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  /**
   * The client configuration.
   */
  private HazelcastMQStompClientConfig config;

  /**
   * The frame input stream used to read all frames.
   */
  private FrameInputStream instream;

  /**
   * The frame output stream used to write all frames.
   */
  private FrameOutputStream outstream;

  /**
   * The flag which indicates if this client has been requested to shutdown.
   */
  private volatile boolean shutdown;

  /**
   * The socket used to connect to the remote STOMP server.
   */
  private Socket socket;

  /**
   * The thread used to fetch incoming frames.
   */
  private Thread frameFetchThread;

  /**
   * The optional frame listener to which frames will be immediately dispatched.
   */
  private FrameListener frameListener;

  /**
   * The frame queue that will buffer incoming frames until they are consumed
   * via the pull API.
   */
  private BlockingQueue<Frame> frameQueue;

  /**
   * The shutdown latch that blocks shutdown until complete.
   */
  private CountDownLatch shutdownLatch;

  /**
   * Constructs the client with the given configuration. The client will
   * immediately attempt to connect to the remote STOMP server.
   * 
   * @param config
   *          the client configuration
   * @throws IOException
   *           if there is a socket level connection problem
   * @throws StompException
   *           if there is a CONNECT handshake problem
   */
  public HazelcastMQStompClient(HazelcastMQStompClientConfig config)
      throws IOException, StompException {
    this.config = config;

    socket = new Socket(config.getAddress(), config.getPort());
    instream = new FrameInputStream(socket.getInputStream());
    outstream = new FrameOutputStream(socket.getOutputStream());
    shutdown = false;
    this.frameQueue = new ArrayBlockingQueue<Frame>(
        this.config.getFrameMaxQueueCount());
    shutdownLatch = new CountDownLatch(1);

    frameFetchThread = new Thread() {
      public void run() {
        runFrameFetchLoop();
      };
    };
    frameFetchThread.start();

    // Perform the initial connect handshake
    Frame frame = new Frame(Command.STOMP);
    frame.getHeaders().put("accept-version", "1.2");
    frame.getHeaders().put("host", config.getAddress());
    outstream.write(frame);

    Frame response = receive(10, TimeUnit.SECONDS);
    if (response == null) {
    }
    else {
      switch (response.getCommand()) {
      case ERROR:
        shutdown();
        throw new StompException("Error frame returned while connecting.");

      case CONNECTED:
        // All is good. Nothing to see here.
        break;

      default:
        shutdown();
        throw new StompException("Unknown frame returned while connecting.");
      }
    }
  }

  /**
   * Returns true if the socket to the STOMP server is connected and therefore
   * the STOMP client is CONNECTED.
   * 
   * @return true if connected, false otherwise
   */
  public boolean isConnected() {
    return socket.isConnected();
  }

  /**
   * Sends the given frame to the STOMP server. This method is thread-safe so
   * frames may be sent from multiple threads.
   * 
   * @param frame
   *          the frame to send
   * @throws IOException
   */
  public void send(Frame frame) throws IOException {
    outstream.write(frame);
  }

  /**
   * Receives the next available frame if there is one, otherwise the method
   * returns null immediately.
   * 
   * @return the available frame or null
   */
  public Frame receiveNoWait() {
    return frameQueue.poll();
  }

  /**
   * Receives the next available frame, blocking up to the given timeout. Null
   * is returned if no frame is available before the timeout.
   * 
   * @param timeout
   *          the maximum time to block waiting for a frame
   * @param unit
   *          the time unit
   * @return the frame or null
   * @throws StompException
   *           if there is an error waiting for a frame
   */
  public Frame receive(long timeout, TimeUnit unit) throws StompException {
    try {
      return frameQueue.poll(timeout, unit);
    }
    catch (InterruptedException ex) {
      throw new StompException("Interrupted while polling.", ex);
    }
  }

  /**
   * Shuts down the client, sending the DISCONNECT frame to the server. This
   * method will block until the client is completely shutdown.
   */
  public void shutdown() {
    shutdown = true;

    // If we're still connected, try to issue a disconnect frame.
    safeDisconnect();

    // Cleanup
    safeClose(instream);
    safeClose(outstream);
    safeClose(socket);

    // Wait for the fetch thread to terminate.
    safeAwait(shutdownLatch, 30, TimeUnit.SECONDS);
  }

  /**
   * Sends the DISCONNECT frame to the server, ignoring any exceptions.
   */
  private void safeDisconnect() {
    try {
      Frame frame = new Frame(Command.DISCONNECT);
      send(frame);
    }
    catch (Exception ex) {
      // Ignore and just close the socket.
    }
  }

  /**
   * Runs the main frame fetch loop, reading frames off the input stream and
   * queuing them for the pull API or immediately pushing them to the listener.
   */
  private void runFrameFetchLoop() {
    while (!shutdown) {
      try {
        Frame frame = instream.read();
        onFrameReceived(frame);
      }
      catch (Exception ex) {
        if (!shutdown) {
          // Log this as an error
          log.warn("Exception in frame receive thread. Client will shutdown.",
              ex);

          // Force a shutdown
          shutdown = true;
        }
      }
    }

    shutdownLatch.countDown();
  }

  /**
   * Sets the frame listener to receive frames as soon as they arrive. Only a
   * single thread services the listener so if the frame takes a long time to
   * process, it should be handled in a separate thread. If this client was
   * already subscribed and has been receiving messages, the messages could be
   * lost when the listener is set. To remove the listener, set it to null.
   * 
   * @param frameListener
   *          the frame listener to set or null to clear it
   */
  public void setFrameListener(FrameListener frameListener) {
    this.frameListener = frameListener;

    // It is possible that we could lose frames here if the client was already
    // receiving messages before setting the listener; however this isn't a
    // supported operation.
    frameQueue.clear();
  }

  /**
   * Called when a new frame is received from the input stream. The frame will
   * be queued or dispatched if there is a listener.
   * 
   * @param frame
   *          the frame received
   */
  private void onFrameReceived(Frame frame) {
    try {
      // Dispatched immediately to the listener if there is one.
      if (frameListener != null) {
        frameListener.frameReceived(frame);
      }
      // Queue it up if there is no listener and assume that someone is going to
      // dequeue it.
      else if (!frameQueue.offer(frame, 2, TimeUnit.SECONDS)) {
        log.warn("Unable to queue incoming frame. The max frame queue count or "
            + "message consumption performance must increase. Frames will be lost.");
      }
    }
    catch (Throwable ex) {
      // Ignore and wait for the next frame.
    }
  }
}
