
package org.mpilone.stomp.example;

import java.util.*;

import org.mpilone.stomp.shared.Frame;
import org.mpilone.stomp.shared.FrameBuilder;
import org.mpilone.stomp.shared.Headers;

import io.netty.channel.Channel;

/**
 *
 * @author mpilone
 */
public class InMemoryBroker {

  private static int subscriberCount;
  private static int messageCount;

  private final Map<String, List<Subscriber>> subscribers =
      new HashMap<String, List<Subscriber>>();

  public synchronized int subscribe(String subscriptionId, String destination,
      Channel channel) {
    Subscriber subscriber = new Subscriber(subscriberCount++, subscriptionId,
        destination, channel);

    if (!subscribers.containsKey(destination)) {
      subscribers.put(destination, new ArrayList<Subscriber>());
    }

    subscribers.get(destination).add(subscriber);

    return subscriber.getId();
  }

  public synchronized void unsubscribe(int subscriberId) {
    for (List<Subscriber> subs : subscribers.values()) {
      for (Iterator<Subscriber> iter = subs.iterator(); iter.hasNext();) {
        Subscriber sub = iter.next();

        if (sub.getId() == subscriberId) {
          iter.remove();
        }
      }
    }
  }

  public synchronized void publish(String destination, String contentType,
      byte[] body) {

    List<Subscriber> subs = subscribers.get(destination);
    if (subs != null && !subs.isEmpty()) {
      // Choose a random subscriber to simulate a fair queue.
      int pos = (int) (Math.random() * subs.size());
      Subscriber sub = subs.get(pos);

      Frame frame = FrameBuilder.message(destination, String.valueOf(
          messageCount++), sub.getSubscriptionId(), body).header(
              Headers.CONTENT_TYPE, contentType).build();

      sub.getChannel().writeAndFlush(frame);
    }
  }

  public static class Subscriber {

    private final int id;
    private final String destination;
    private final Channel channel;
    private final String subscriptionId;

    public Subscriber(int subscriberId, String subscriptionId,
        String destination, Channel channel) {
      this.id = subscriberId;
      this.subscriptionId = subscriptionId;
      this.destination = destination;
      this.channel = channel;
    }

    public String getSubscriptionId() {
      return subscriptionId;
    }

    public Channel getChannel() {
      return channel;
    }

    public String getDestination() {
      return destination;
    }

    public int getId() {
      return id;
    }
  }
}
