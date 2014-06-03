package org.mpilone.stomp.client;

import static java.lang.String.format;

import org.mpilone.stomp.*;

/**
 *
 * @author mpilone
 */
public class ClientStomplet extends GenericStomplet {

 

  @Override
  public void service(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    //
    // TODO: handle null frame or null command for heartbeat?
    //
    // Check for a bad frame from the client.
    if (frame.getHeaders().getHeaderNames().contains(
        StompFrameDecoder.HEADER_BAD_REQUEST)) {
      throw new StompException(frame.getHeaders().get(
          StompFrameDecoder.HEADER_BAD_REQUEST), null, req.getFrame());
    }

    switch (frame.getCommand()) {
      case RECEIPT:
        doReceipt(req, res);
        break;

      case MESSAGE:
        doMessage(req, res);
        break;

      case ERROR:
        doError(req, res);
        break;

      case CONNECTED:
        doConnected(req, res);
        break;

      default:
        throw new StompException("Unsupported frame command.", format(
            "The command %s is not supported.", frame.getCommand()), frame);
    }
  }

  @Override
  public void destroy() {
    // no op
  }

  protected void doConnected(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doMessage(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doError(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doReceipt(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }
}
