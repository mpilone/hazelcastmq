/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mpilone.hazelcastmq.core;

import com.hazelcast.core.BaseMap;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.*;

/**
 *
 * @author mpilone
 */
abstract class MessageSentMapAdapter implements
    EntryAddedListener<DataStructureKey, Long>,
    EntryUpdatedListener<DataStructureKey, Long>,
    EntryMergedListener<DataStructureKey, Long> {

  public final static String MESSAGE_SENT_MAP_NAME =
      "hzmq.messagesentmap";

  @Override
  public void entryAdded(EntryEvent<DataStructureKey, Long> event) {
    messageSent(event.getKey());
  }

  @Override
  public void entryMerged(EntryEvent<DataStructureKey, Long> event) {
    messageSent(event.getKey());
  }

  @Override
  public void entryUpdated(EntryEvent<DataStructureKey, Long> event) {
    messageSent(event.getKey());
  }

  abstract void messageSent(DataStructureKey channelKey);

  public static IMap<DataStructureKey, Long> getMapToListen(
      DataStructureContext context) {

    final BaseMap<DataStructureKey, Long> map = context.getMap(
        MESSAGE_SENT_MAP_NAME, false);
    return (IMap<DataStructureKey, Long>) map;
  }

  public static BaseMap<DataStructureKey, Long> getMapToPut(
      DataStructureContext context, boolean joinTransaction) {

    final BaseMap<DataStructureKey, Long> map = context.getMap(
        MESSAGE_SENT_MAP_NAME, joinTransaction);
    return map;
  }

}
