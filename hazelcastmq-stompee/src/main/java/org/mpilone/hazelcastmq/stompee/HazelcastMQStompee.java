package org.mpilone.hazelcastmq.stompee;

import static org.mpilone.hazelcastmq.stomp.IoUtil.safeClose;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.mpilone.hazelcastmq.stomp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastMQStompee {

  /**
   * The log for this class.
   */
  private final Logger log = LoggerFactory.getLogger(getClass());

  private HazelcastMQStompeeConfig config;
  private FrameInputStream instream;
  private FrameOutputStream outstream;
  private volatile boolean shutdown;
  private Socket socket;
  private Thread frameFetchThread;
  private FrameListener frameListener;

  public HazelcastMQStompee(HazelcastMQStompeeConfig config) throws IOException {
    this.config = config;

    socket = new Socket(config.getAddress(), config.getPort());
    instream = new FrameInputStream(socket.getInputStream());
    outstream = new FrameOutputStream(socket.getOutputStream());
    shutdown = false;

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

  public boolean isConnected() {
    return socket.isConnected();
  }

  public void send(Frame frame) throws IOException {
    synchronized (outstream) {
      outstream.write(frame);
    }
  }

  public Frame receiveNoWait() {
    return config.getFrameFetchQueue().poll();
  }

  public Frame receive(long timeout, TimeUnit unit) throws StompException {
    try {
      return config.getFrameFetchQueue().poll(timeout, unit);
    }
    catch (InterruptedException ex) {
      throw new StompException("Interrupted while polling.", ex);
    }
  }

  public void shutdown() {
    shutdown = true;

    // If we're still connected, try to issue a disconnect frame.
    safeDisconnect();

    // Cleanup
    safeClose(instream);
    safeClose(outstream);
    safeClose(socket);

    try {
      // Wait for the fetch thread to terminate.
      frameFetchThread.join(5000);
    }
    catch (Exception ex) {
      // Ignore
    }
  }

  private void safeDisconnect() {
    try {
      Frame frame = new Frame(Command.DISCONNECT);
      send(frame);
    }
    catch (Exception ex) {
      // Ignore and just close the socket.
    }
  }

  protected void runFrameFetchLoop() {
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
  }

  private void onFrameReceived(Frame frame) {
    try {
      // Dispatched immediately to the listener if there is one.
      if (frameListener != null) {
        frameListener.frameReceived(frame);
      }
      // Queue it up if there is no listener and assume that someone is going to
      // dequeue it.
      else if (!config.getFrameFetchQueue().offer(frame, 2, TimeUnit.SECONDS)) {
        log.warn("Unable to queue incoming message. The frame fetch queue size or "
            + "message consumption must increase. Frames will be lost.");
      }
    }
    catch (Throwable ex) {
      // Ignore and wait for the next frame.
    }
  }
}
