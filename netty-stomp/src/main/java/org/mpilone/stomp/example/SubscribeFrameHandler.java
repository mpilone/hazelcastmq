
package org.mpilone.stomp.example;

import static java.lang.String.format;

import java.util.*;

import org.mpilone.stomp.shared.*;

import io.netty.channel.*;

/**
 *
 * @author mpilone
 */
public class SubscribeFrameHandler extends SimpleChannelInboundHandler<Frame> {

  private final InMemoryBroker broker;
  private final Map<String, Integer> subscriptionMap;

  public SubscribeFrameHandler(InMemoryBroker broker) {
    super(Frame.class, true);

    this.broker = broker;
    this.subscriptionMap = new HashMap<String, Integer>();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws
      Exception {

    String id;

    switch (frame.getCommand()) {
      case SUBSCRIBE:
        String destination = frame.getHeaders().get(Headers.DESTINATION);
         id = frame.getHeaders().get(Headers.ID);

        if (subscriptionMap.containsKey(id)) {
          throw new StompClientException("Subscription ID already in use.",
              format("Subscription ID [%s] is already in use by the client.", id),
              frame);
        }

        int subscriberId = broker.subscribe(id, destination, ctx.channel());
        subscriptionMap.put(id, subscriberId);
        break;

      case UNSUBSCRIBE:
        id = frame.getHeaders().get(Headers.ID);

        if (!subscriptionMap.containsKey(id)) {
          throw new StompClientException("Subscription ID not in use.",
              format("Subscription ID [%s] is not in use by the client.", id),
              frame);
        }

        broker.unsubscribe(subscriptionMap.remove(id));
        break;

      default:
        // no op
        break;
    }

    ctx.fireChannelRead(frame);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.println("SubscribeFrameHandler: Channel inactive.");

    // Clean up any subscriptions for this client.
    for (String subscriptionId : subscriptionMap.keySet()) {
      broker.unsubscribe(subscriptionMap.remove(subscriptionId));
    }

    super.channelInactive(ctx);
  }

}
