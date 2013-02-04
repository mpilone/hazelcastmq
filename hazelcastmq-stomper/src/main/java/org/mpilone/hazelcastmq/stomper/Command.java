package org.mpilone.hazelcastmq.stomper;

/**
 * Possible STOMP commands, both server and client.
 * 
 * @author mpilone
 */
enum Command {
  SEND, SUBSCRIBE, UNSUBSCRIBE, BEGIN, COMMIT, ABORT, ACK, NACK, DISCONNECT, CONNECT, STOMP, CONNECTED, MESSAGE, RECEIPT, ERROR

}
