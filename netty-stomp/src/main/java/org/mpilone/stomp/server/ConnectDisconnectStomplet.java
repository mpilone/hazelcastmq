
package org.mpilone.stomp.server;

import org.mpilone.stomp.*;

/**
 * A {@link Stomplet} implementation that handles the {@link Command#CONNECT}
 * and {@link Command#DISCONNECT} frames. The stomplet also enforces the correct
 * connection state before allowing any other frames to be processed.
 *
 * @author mpilone
 */
public class ConnectDisconnectStomplet extends ServerStomplet {

  private boolean connected = false;

  @Override
  public void service(StompletRequest req, StompletResponse res) throws
      Exception {
    Frame frame = req.getFrame();

    if (frame != null && frame.getCommand() != null) {
      switch (frame.getCommand()) {
        case CONNECT:
        case STOMP:
          if (connected) {
            throw new StompClientException("Client already connected.");
          }
          break;

        default:
          if (!connected) {
            throw new StompClientException("Client must be connected.");
          }
          break;
      }
    }

    super.service(req, res);
  }

  @Override
  protected void doConnect(StompletRequest req, StompletResponse res) throws
      Exception {
     // TODO: perform content negotiation, authentication,
    // heart-beat setup, etc.
    connected = true;
    res.getFrameChannel().write(FrameBuilder.connected("1.2").build());
  }

  @Override
  protected void doDisconnect(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
    res.setFinalResponse(true);
    connected = false;
  }

  @Override
  public void destroy() {
    connected = false;
    super.destroy();
  }
}
