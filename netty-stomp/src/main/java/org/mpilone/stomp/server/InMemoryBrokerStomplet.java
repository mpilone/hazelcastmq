
package org.mpilone.stomp.server;

import static java.lang.String.format;

import java.util.*;

import org.mpilone.stomp.*;

/**
 *
 * @author mpilone
 */
public class InMemoryBrokerStomplet extends ConnectDisconnectStomplet {

  /**
   * The in-memory broker that is shared by all implementations of this stomplet
   * to support message publishing across clients.
   */
  private static final InMemoryBroker BROKER = new InMemoryBroker();

  /**
   * The map of client subscription IDs to the in-memory broker subscription ID.
   * The broker generates a unique ID for every subscription while different
   * clients may reuse the same IDs.
   */
  private final Map<String, String> subscriptionMap = new HashMap<>();

  @Override
  protected void doSubscribe(StompletRequest req, StompletResponse res) throws
      Exception {
    Frame frame = req.getFrame();

    String destination = frame.getHeaders().get(Headers.DESTINATION);
    String clientSubId = frame.getHeaders().get(Headers.ID);

    if (subscriptionMap.containsKey(clientSubId)) {
      throw new StompClientException("Subscription ID already in use.",
          format("Subscription ID [%s] is already in use by the client.",
              clientSubId),
          frame);
    }

    String brokerSubId = BROKER.subscribe(clientSubId, destination, res.
        getFrameChannel());
    subscriptionMap.put(clientSubId, brokerSubId);
    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doUnsubscribe(StompletRequest req, StompletResponse res) throws
      Exception {
    Frame frame = req.getFrame();
    String clientSubId = frame.getHeaders().get(Headers.ID);

    if (!subscriptionMap.containsKey(clientSubId)) {
      throw new StompClientException("Subscription ID not in use.",
          format("Subscription ID [%s] is not in use by the client.",
              clientSubId),
          frame);
    }

    BROKER.unsubscribe(subscriptionMap.remove(clientSubId));
    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  protected void doSend(StompletRequest req, StompletResponse res) throws
      Exception {
    Frame frame = req.getFrame();

    String destination = frame.getHeaders().get(Headers.DESTINATION);
    String contentType = frame.getHeaders().get(Headers.CONTENT_TYPE);

    if (contentType == null) {
      contentType = "application/octet-stream";
    }

    // Create a message frame and send it via the broker.
    BROKER.publish(destination, contentType, frame.getBody());
    writeOptionalReceipt(frame, res.getFrameChannel());
  }

  @Override
  public void destroy() {
    // Clean up any subscriptions for this client.
    for (String subscriptionId : subscriptionMap.keySet()) {
      BROKER.unsubscribe(subscriptionMap.get(subscriptionId));
    }
    subscriptionMap.clear();
  }

  /**
   * A simple in-memory message broker that can track subscriptions and publish
   * messages to subscribers. This class is for demonstration and testing
   * purposes only and should not be considered production usable.
   *
   * @author mpilone
   */
  public static class InMemoryBroker {

    private final Map<String, List<Subscriber>> subscribers = new HashMap<>();

    public synchronized String subscribe(String subscriptionId,
        String destination,
        WritableFrameChannel channel) {
      Subscriber subscriber = new Subscriber(UUID.randomUUID().toString(),
          subscriptionId, destination, channel);

      if (!subscribers.containsKey(destination)) {
        subscribers.put(destination, new ArrayList<Subscriber>());
      }

      subscribers.get(destination).add(subscriber);

      return subscriber.getBrokerSubId();
    }

    public synchronized void unsubscribe(String brokerSubId) {
      for (List<Subscriber> subs : subscribers.values()) {
        for (Iterator<Subscriber> iter = subs.iterator(); iter.hasNext();) {
          Subscriber sub = iter.next();

          if (sub.getBrokerSubId().equals(brokerSubId)) {
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

        Frame frame = FrameBuilder.message(destination, UUID.randomUUID().
            toString(), sub.getClientSubId(), body).header(
                Headers.CONTENT_TYPE, contentType).build();

        sub.getChannel().write(frame);
      }
    }

    public static class Subscriber {

      private final String brokerSubId;
      private final String destination;
      private final WritableFrameChannel channel;
      private final String clientSubId;

      public Subscriber(String brokerSubId, String clientSubId,
          String destination, WritableFrameChannel channel) {
        this.brokerSubId = brokerSubId;
        this.clientSubId = clientSubId;
        this.destination = destination;
        this.channel = channel;
      }

      public String getClientSubId() {
        return clientSubId;
      }

      public WritableFrameChannel getChannel() {
        return channel;
      }

      public String getDestination() {
        return destination;
      }

      public String getBrokerSubId() {
        return brokerSubId;
      }
    }
  }
}
