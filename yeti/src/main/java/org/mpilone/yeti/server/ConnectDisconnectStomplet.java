
package org.mpilone.yeti.server;

import static org.mpilone.yeti.Command.CONNECT;
import static org.mpilone.yeti.Command.STOMP;

import org.mpilone.yeti.*;
import org.mpilone.yeti.Stomplet.StompletRequest;
import org.mpilone.yeti.Stomplet.StompletResponse;

/**
 * A server side {@link Stomplet} implementation that handles the
 * {@link Command#CONNECT} and {@link Command#DISCONNECT} frames. The stomplet
 * also enforces the correct connection state before allowing any other frames
 * to be serviced.
 *
 * @author mpilone
 */
public class ConnectDisconnectStomplet extends ServerStomplet {

  /**
   * The flag which indicates if the connection has been established to the
   * remote host (at the STOMP level).
   */
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
