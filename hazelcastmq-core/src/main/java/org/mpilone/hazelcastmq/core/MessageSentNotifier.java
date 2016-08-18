
package org.mpilone.hazelcastmq.core;

import java.util.EventListener;

/**
 *
 * @author mpilone
 */
interface MessageSentNotifier {

  void fireMessageSent(DataStructureKey key);

  String addMessageSentListener(MessageSentListener listener);

  String addMessageSentListener(MessageSentListener listener,
      DataStructureKey key);

  boolean removeMessageSentListener(String id);

  interface MessageSentListener extends EventListener {

    void messageSent(MessageSentEvent event);
  }

  static class MessageSentEvent {

    private final DataStructureKey channelKey;

    public MessageSentEvent(DataStructureKey channelKey) {
      this.channelKey = channelKey;
    }

    public DataStructureKey getChannelKey() {
      return channelKey;
    }
  }

}
