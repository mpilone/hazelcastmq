package org.mpilone.yeti.server;

import static java.lang.String.format;
import static org.mpilone.yeti.Command.*;

import org.mpilone.yeti.*;
import org.mpilone.yeti.Stomplet.StompletRequest;
import org.mpilone.yeti.Stomplet.StompletResponse;
import org.mpilone.yeti.Stomplet.WritableFrameChannel;


/**
 * A server side implementation of {@link Stomplet} that delegates server frames
 * to handler methods. If the frame was detected to be malformed or the frame
 * command is not valid for a client to server request, a
 * {@link StompClientException} will be raised.
 *
 * @author mpilone
 */
public class ServerStomplet extends GenericStomplet {

  @Override
  public void service(StompletRequest req, StompletResponse res) throws
      Exception {

    Frame frame = req.getFrame();

    // Check for a bad frame from the client.
    if (frame.getHeaders().getHeaderNames().contains(
        StompFrameDecoder.HEADER_BAD_REQUEST)) {
      throw new StompClientException(frame.getHeaders().get(
          StompFrameDecoder.HEADER_BAD_REQUEST), null, req.getFrame());
    }

    switch (frame.getCommand()) {
      case CONNECT:
      case STOMP:
        doConnect(req, res);
        break;

      case ABORT:
        doAbort(req, res);
        break;

      case BEGIN:
        doBegin(req, res);
        break;

      case COMMIT:
        doCommit(req, res);
        break;

      case DISCONNECT:
        doDisconnect(req, res);
        break;

      case SEND:
        doSend(req, res);
        break;

      case SUBSCRIBE:
        doSubscribe(req, res);
        break;

      case UNSUBSCRIBE:
        doUnsubscribe(req, res);
        break;

      case ACK:
        doAck(req, res);
        break;

      case NACK:
        doNack(req, res);
        break;

      default:
        throw new StompClientException("Unsupported frame command.", format(
            "The command %s is not supported.", frame.getCommand()), frame);
    }
  }

  @Override
  public void destroy() {
    // no op
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#CONNECT} frame. The default implementation
   * throws a {@link StompException} indicating that the operation is not
   * supported.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doConnect(StompletRequest req, StompletResponse res) throws
      Exception {
    throw new StompException("CONNECT is not implemented.", null, req.getFrame());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#ABORT} frame. The default implementation will
   * write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doAbort(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#BEGIN} frame. The default implementation will
   * write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doBegin(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#COMMIT} frame. The default implementation will
   * write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doCommit(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#DISCONNECT} frame. The default implementation
   * will write a receipt for the frame if one has been requested and mark the
   * response as final.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doDisconnect(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
    res.setFinalResponse(true);
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#SEND} frame. The default implementation will
   * write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doSend(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#SUBSCRIBE} frame. The default implementation
   * will write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doSubscribe(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#UNSUBSCRIBE} frame. The default implementation
   * will write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doUnsubscribe(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#ACK} frame. The default implementation will
   * write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doAck(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Called by the stomplet (via the {@code service} method) to allow a stomplet
   * to handle a {@link Command#NACK} frame. The default implementation will
   * write a receipt for the frame if one has been requested.
   *
   * @param req the {@link StompletRequest} object that contains the frame to be
   * serviced
   * @param res the {@link StompletResponse} object that contains the frame
   * channel to write to
   *
   * @throws Exception if an error occurs while handling the request
   */
  protected void doNack(StompletRequest req, StompletResponse res) throws
      Exception {
    writeOptionalReceipt(req.getFrame(), res.getFrameChannel());
  }

  /**
   * Writes an optional receipt frame for the given frame if one has been
   * requested. That is, if the given frame contains a {@link Headers#RECEIPT}
   * value, a receipt frame will be written to the frame channel. It is safe to
   * call this method on null frames or frames that may not be requesting a
   * receipt and they will simply be ignored. Normally this method is called by
   * one of the frame handling methods in this class but it can be called by
   * subclasses for manual receipt management. If calling the method directly,
   * be sure not to call the super method or double receipts could be written.
   *
   * @param frame the frame to potentially send a receipt for
   * @param channel the frame channel to write the receipt to
   */
  protected void writeOptionalReceipt(Frame frame, WritableFrameChannel channel) {
    String receiptId = frame != null ? frame.getHeaders().get(Headers.RECEIPT) :
        null;

    if (receiptId != null) {
      channel.write(FrameBuilder.receipt(receiptId).build());
    }
  }
}
