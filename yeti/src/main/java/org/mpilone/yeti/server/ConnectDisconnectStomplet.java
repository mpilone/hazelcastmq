package org.mpilone.yeti.server;

import static java.lang.String.format;
import static org.mpilone.yeti.Command.CONNECT;
import static org.mpilone.yeti.Command.STOMP;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

  private static final int HEARTBEAT_SR_SEND = (int) TimeUnit.SECONDS.toMillis(
      30);
  private static final int HEARTBEAT_SR_RECV = (int) TimeUnit.SECONDS.toMillis(
      30);

  private boolean connected = false;
  private StompVersion version;
  private final StompVersion[] supportedVersions;

  /**
   * Constructs the stomplet. By default, all protocol versions defined in
   * {@link StompVersion} are supported.
   */
  public ConnectDisconnectStomplet() {
    this(StompVersion.values());
  }

  /**
   * Constructs the stomplet supporting only the given protocol versions.
   *
   * @param supportedVersions the protocol versions supported
   */
  public ConnectDisconnectStomplet(StompVersion[] supportedVersions) {
    this.supportedVersions = supportedVersions;
    StompVersion.sortDescending(this.supportedVersions);
  }

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
    Frame frame = req.getFrame();
    version = processAcceptHeader(frame);

    FrameBuilder resFrameBuilder = FrameBuilder.connected(version);

    try {
      preConnect(version, frame, resFrameBuilder);
      connected = true;
      postConnect(version, frame, resFrameBuilder);
    }
    catch (Exception ex) {
      connected = false;
      version = null;

      throw ex;
    }

    res.getFrameChannel().write(resFrameBuilder.build());
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

  /**
   * Returns the flag which indicates if the connection has been established to
   * the remote host (at the STOMP layer) via the CONNECT/CONNECTED frames.
   *
   * @return true if the connection has been negotiated and established (at the
   * STOMP layer)
   */
  protected boolean isConnected() {
    return connected;
  }

  /**
   * Returns the STOMP protocol version negotiated with the client during the
   * connect operation. If a client is not connected, this method will return
   * null.
   *
   * @return the protocol version negotiated
   */
  protected StompVersion getVersion() {
    return version;
  }

  /**
   * <p>
   * Called after protocol version negotiation but before the connect frame has
   * been processed and the client is considered not connected. This method can
   * be overridden in subclasses to process headers such as
   * {@link Headers#LOGIN}, {@link Headers#PASSCODE}, {@link Headers#HOST}. The
   * default implementation checks for required headers based on the protocol
   * version and calls the {@link #processHeartbeatHeader(org.mpilone.yeti.Frame, org.mpilone.yeti.FrameBuilder)
   * } method to establish the heartbeat. It is recommended that this
   * implementation by subclasses.
   * </p>
   * <p>
   * <strong>WARNING:</strong> the default implementation does not perform any
   * authentication. It is up to a subclass implementation to implement security
   * if required.
   * </p>
   *
   * @param version the protocol version negotiated during connect
   * @param frame the incoming connect frame
   * @param connectedFrameBuilder the connected response frame builder to add
   * response headers to
   *
   * @throws StompClientException if there is a problem with the request such as
   * invalid credentials or a missing required header
   */
  protected void preConnect(StompVersion version, Frame frame,
      FrameBuilder connectedFrameBuilder) throws StompClientException {

    switch (version) {
      case VERSION_1_2:
      case VERSION_1_1:
        // Check for required headers.
        String value = frame.getHeaders().get(Headers.HOST);
        if (value == null) {
          throw new StompClientException(format("Header %s is required.",
              Headers.HOST));
        }

        // Apply the heart-beat if supported.
        processHeartbeatHeader(frame, connectedFrameBuilder);
        break;

      default:
        // no-op.
        break;
    }
  }

  /**
   * Called after the connect frame has been fully processed and the client is
   * considered connected but before the connected response frame is written.
   * This method can be overridden in subclasses to set any headers such as
   * {@link Headers#SERVER}. The default implementation sets the
   * {@link Headers#SERVER} header to "Yeti/version".
   *
   * @param version the protocol version negotiated during connect
   * @param frame the incoming connect frame
   * @param connectedFrameBuilder the connected response frame builder to add
   * response headers to
   */
  protected void postConnect(StompVersion version, Frame frame,
      FrameBuilder connectedFrameBuilder) {
    connectedFrameBuilder.header(Headers.SERVER, "Yeti/1.x");
  }

  /**
   * Processes the {@link Headers#ACCEPT_VERSION} header by negotiation to the
   * highest protocol in common.
   * }.
   *
   * @param frame the incoming connect frame to process
   *
   * @return the highest protocol version in common
   * @throws StompClientException if the header could not be parsed or no
   * version was found in common
   */
  protected StompVersion processAcceptHeader(Frame frame) throws
      StompClientException {

    Set<StompVersion> versions = new HashSet<>(3);

    // Parse the accept-version header.
    String acceptVersion = frame.getHeaders().get(Headers.ACCEPT_VERSION);
    if (acceptVersion != null) {
      String[] values = acceptVersion.trim().split(",");

      for (String value : values) {
        try {
          versions.add(StompVersion.fromHeaderValue(value));
        }
        catch (IllegalArgumentException ex) {
          throw new StompClientException(
              format("Illegal value in %s header: %s", Headers.ACCEPT_VERSION,
                  value), null, frame, ex);
        }
      }
    }
    else {
      // Default to 1.0 if no accept-version header is present.
      versions.add(StompVersion.VERSION_1_0);
    }

    // Find the highest protocol version in common.
    for (StompVersion v : supportedVersions) {
      if (versions.contains(v)) {
        return v;
      }
    }

    // No common protocol version. Raise an error.
    String msg = format("Supported protocol versions are %s", Arrays.toString(
        supportedVersions));
    throw new StompClientException(msg, msg, null);
  }

  /**
   * Processes the {@link Headers#HEART_BEAT} header on the connect frame to
   * establish the heartbeat interval. The heartbeat will be initialized in the
   * {@link StompletContext} if one is supported.
   *
   * @param frame the connect frame to process
   * @param connectedFrameBuilder the connected response frame builder to add
   * response headers to
   *
   * @throws StompClientException if the header value is invalid or cannot be
   * parsed
   */
  protected void processHeartbeatHeader(Frame frame,
      FrameBuilder connectedFrameBuilder) throws StompClientException {

    // Defaults.
    int clSend = 0; // cx
    int clRecv = 0; // cy
    int srSend = HEARTBEAT_SR_SEND; // sx
    int srRecv = HEARTBEAT_SR_RECV; // sy

    // Get the heart-beat header and parse it.
    String heartbeat = frame.getHeaders().get(Headers.HEART_BEAT);
    if (heartbeat != null) {
      String[] values = heartbeat.trim().split(",");
      if (values.length != 2) {
        throw new StompClientException("Invalid heart-beat header.", null, frame);
      }

      try {
        clSend = Integer.parseInt(values[0]);
        clRecv = Integer.parseInt(values[1]);
      }
      catch (NumberFormatException ex) {
        throw new StompClientException("Invalid values for heart-beat header.",
            null, frame, ex);
      }
    }

    int clientToServerInterval = clSend == 0 ? 0 : Math.max(clSend,
        srRecv);
    int serverToClientInterval = clRecv == 0 ? 0 : Math.max(clRecv,
        srSend);

    connectedFrameBuilder.header(Headers.HEART_BEAT, format("%d,%d",
        serverToClientInterval, clientToServerInterval));

    // Init the heartbeat in the stomplet context.
    getStompletContext().configureHeartbeat(clientToServerInterval,
        serverToClientInterval);
  }
}
