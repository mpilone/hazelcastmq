package org.mpilone.stomp.server;

import static java.lang.String.format;

import org.mpilone.stomp.*;

/**
 *
 * @author mpilone
 */
public class ServerStomplet implements Stomplet {

  @Override
  public void init() {
    // no op
  }

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

      default:
        throw new StompClientException("Unsupported frame command.", format(
            "The command %s is not supported.", frame.getCommand()), frame);
    }
  }

  @Override
  public void destroy() {
    // no op
  }

  protected void doConnect(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doAbort(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doBegin(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doCommit(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doDisconnect(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doSend(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doSubscribe(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void doUnsubscribe(StompletRequest req, StompletResponse res) throws
      Exception {
    // no op
  }

  protected void writeOptionalReceipt(Frame frame, WritableFrameChannel channel) {
    String receiptId = frame != null ? frame.getHeaders().get(Headers.RECEIPT) :
        null;

    if (receiptId != null) {
      channel.write(FrameBuilder.receipt(receiptId).build());
    }
  }
}
