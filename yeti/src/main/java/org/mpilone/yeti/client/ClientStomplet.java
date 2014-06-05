package org.mpilone.yeti.client;

import static java.lang.String.format;
import static org.mpilone.yeti.Command.CONNECTED;
import static org.mpilone.yeti.Command.ERROR;

import org.mpilone.yeti.*;
import org.mpilone.yeti.Stomplet.StompletRequest;
import org.mpilone.yeti.Stomplet.StompletResponse;


/**
 * A client side implementation of {@link Stomplet} that delegates client frames
 * to handler methods. If the frame was detected to be malformed or the frame
 * command is not valid for a client to server request, a {@link StompException}
 * will be raised.
 *
 * @author mpilone
 */
public class ClientStomplet extends GenericStomplet {

  @Override
  public void service(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

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

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#CONNECTED} frame. The default implementation
   * does nothing.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doConnected(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#MESSAGE} frame. The default implementation does
   * nothing.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doMessage(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#ERROR} frame. The default implementation does
   * nothing.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doError(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#RECEIPT} frame. The default implementation does
   * nothing.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doReceipt(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }
}
