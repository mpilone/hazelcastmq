# Transactions and Message Ack

## JMS (6.2.10)
  - If transacted, acknowledgement is based on commit/rollback
  - If not transacted:
    - DUPS_OK: lazy acknowledge
 	- AUTO: acknowledge after receive or message listener successful
 	- CLIENT: client calls acknowledge on message
  - JMSRedelivered and JMSXDeliveryCount set on recovery

## STOMP
  - transactions can be used with SEND, COMMIT, ABORT, ACK, and NACK frames
  - auto: acknowledge after sending message to client
  - client: client sends ACK frame. All messages up to and including the ack'ed message are acknowledged
  - client-individual: client sends ACK frame. Only ack'ed message is acknowledged
  
# Threading

- Receive blocks but producers can continue to be used
- When using async consumer(s), producers can only be used within the message listeners
- A session (and all consumers) are either async or sync, the behavior cannot be mixed.
- The session must be stopped to setup more than one message listener.
- A session can be used to send messages regardless of started/stopped state.
- Context start/stop block until delivery of messages has paused (and the message listener(s) have returned).
- Thread-safe calls:
  - Context start
  - Context stop
  - Context close
  - Consumer close
- Client receive ops:
  - receive(): blocks until a message is received or consumer closed
  - receive(timeout): blocks until message is received, timeout, or consumer closed
  - receiveNoWait(): returns message if any
- MessageListener cannot call stop or close on its own context
- MessageListener may call close on its own consumer