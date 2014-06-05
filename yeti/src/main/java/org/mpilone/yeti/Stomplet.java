package org.mpilone.yeti;

import org.mpilone.yeti.client.ClientStomplet;
import org.mpilone.yeti.server.ServerStomplet;

/**
 * <p>
 * A stomplet that the {@link StompletFrameHandler} will delegate to for
 * handling incoming STOMP {@link Frame}s. Similar in concept to a servlet that
 * handles HTTP requests, a stomplet handles STOMP request frames and can
 * optionally generate response frames.
 * </p>
 * <p>
 * A stomplet can be used in both server and client implementations and
 * therefore simply operates with the idea of a remote service. When the
 * stomplet is implemented on the server side, the remote service is the client
 * and when the stomplet is implemented on the client side, the remote service
 * is the server. Normally this interface is not implemented directly but a more
 * specific client or server subclass is used to dispatch frames to a specific
 * handler method.
 * </p>
 * <p>
 * A stomplet purposely abstracts away the underlying network implementation to
 * support various network IO implementations. Currently a Netty handler is
 * provided to adapt from a Netty handler pipeline to a Stomplet but other
 * implementations are possible in the future.
 *
 * @author mpilone
 * @see ServerStomplet
 * @see ClientStomplet
 */
public interface Stomplet {

  /**
   * Initializes the stomplet with the given context. This method will be called
   * when a connection (at the network level) to a remote service has been
   * established.
   *
   * @param context the stomplet context
   */
  void init(StompletContext context);

  /**
   * Services the given request frame and can generate zero or more response
   * frames.
   *
   * @param req the request frame
   * @param res the response used to write one or more response frames
   *
   * @throws Exception if there is an error processing the request
   */
  void service(StompletRequest req, StompletResponse res) throws Exception;

  /**
   * Destroys the stomplet and releases any resources. This method will be
   * called when the connection (at the network level) to the remote service is
   * closed.
   */
  void destroy();

  /**
   * A STOMP request including the frame to be processed.
   */
  interface StompletRequest {

    /**
     * Returns the frame in the request.
     *
     * @return the frame
     */
    public Frame getFrame();
  }

  /**
   * A STOMP response including a frame channel that can be used to write
   * optional response frames.
   */
  interface StompletResponse {

    /**
     * Returns the frame channel that can be used to write optional response
     * frames. Unlike an HTTP request/response cycle, a STOMP connection is
     * persistent which means it is safe to hold a reference to the frame
     * channel for future writes at any time.
     *
     * @return the frame channel
     */
    public WritableFrameChannel getFrameChannel();

    /**
     * If set to true, the response will be considered the final response on
     * this connection and the remote service will be terminated after flushing
     * any frames written in the response.
     *
     * @param finalResponse true to indicate that the response is the final one
     * and the connection should be terminated
     */
    public void setFinalResponse(boolean finalResponse);
  }

  /**
   * A channel for writing frames to a remote connection.
   */
  interface WritableFrameChannel {

    /**
     * Writes the given frame to the remote service.
     *
     * @param frame the frame to write
     */
    void write(Frame frame);
  }

  /**
   * The stomplet context.
   */
  interface StompletContext {

  }
}
