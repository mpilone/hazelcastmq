package org.mpilone.hazelcastmq.jms;

import java.io.UnsupportedEncodingException;
import java.util.*;

import javax.jms.*;

/**
 * Base JMS message to be sent over the HazelcastMQ.
 * 
 * @author mpilone
 */
abstract class HazelcastMQJmsMessage implements Message {

  private static final String HEADER_PREFIX_DESTINATION_TYPE = "HzMq-DestType-";
  public static final String HEADER_JMS_EXPIRATION = "JMSExpiration";
  public static final String HEADER_JMS_MESSAGE_ID = "JMSMessageID";
  public static final String HEADER_JMS_PRIORITY = "JMSPriority";
  public static final String HEADER_JMS_REDELIVERED = "JMSRedelivered";
  public static final String HEADER_JMS_REPLY_TO = "JMSReplyTo";
  public static final String HEADER_JMS_TIMESTAMP = "JMSTimestamp";
  public static final String HEADER_JMS_TYPE = "JMSType";
  public static final String HEADER_JMS_CORRELATION_ID = "JMSCorrelationID";
  public static final String HEADER_JMS_DELIVERY_MODE = "JMSDeliveryMode";
  public static final String HEADER_JMS_DESTINATION = "JMSDestination";

  /**
   * The user defined properties of the message.
   */
  private final Map<String, String> properties;

  /**
   * The standard JMS headers of the message.
   */
  private final Map<String, String> headers;

  /**
   * Constructs a message with no headers or properties.
   */
  public HazelcastMQJmsMessage() {
    properties = new HashMap<>();
    headers = new HashMap<>();
  }

  @Override
  public void acknowledge() throws JMSException {
    // no op
  }

  @Override
  public void clearBody() throws JMSException {
    // no op
  }

  @Override
  public void clearProperties() throws JMSException {
    properties.clear();
  }

  @Override
  public boolean getBooleanProperty(String name) throws JMSException {
    return Boolean.valueOf(getStringProperty(name));
  }

  @Override
  public byte getByteProperty(String name) throws JMSException {
    return Byte.valueOf(getStringProperty(name));
  }

  @Override
  public double getDoubleProperty(String name) throws JMSException {
    return Double.valueOf(getStringProperty(name));
  }

  @Override
  public float getFloatProperty(String name) throws JMSException {
    return Float.valueOf(getStringProperty(name));
  }

  @Override
  public int getIntProperty(String name) throws JMSException {
    return Integer.valueOf(getStringProperty(name));
  }

  @Override
  public String getJMSCorrelationID() throws JMSException {
    return getHeader(HEADER_JMS_CORRELATION_ID, null);
  }

  /**
   * Returns the value of the header with the given name or the default value if
   * the header doesn't exist.
   * 
   * @param name
   *          the name of the header
   * @param defaultValue
   *          the default value if the header doesn't exist
   * @return the header value or the default value
   * @throws JMSException
   */
  private String getHeader(String name, String defaultValue)
      throws JMSException {
    String value = headers.get(name);
    if (value == null) {
      value = defaultValue;
    }

    return value;
  }

  /**
   * Returns the value of the header with the given name or the default value if
   * the header doesn't exist.
   * 
   * @param name
   *          the name of the header
   * @param defaultValue
   *          the default value if the header doesn't exist
   * @return the header value or the default value
   * @throws JMSException
   */
  private int getHeader(String name, int defaultValue) throws JMSException {
    String value = getHeader(name, (String) null);
    if (value == null) {
      return defaultValue;
    }
    else {
      return Integer.valueOf(value);
    }
  }

  /**
   * Returns the value of the header with the given name or the default value if
   * the header doesn't exist.
   * 
   * @param name
   *          the name of the header
   * @param defaultValue
   *          the default value if the header doesn't exist
   * @return the header value or the default value
   * @throws JMSException
   */
  private long getHeader(String name, long defaultValue) throws JMSException {
    String value = getHeader(name, (String) null);
    if (value == null) {
      return defaultValue;
    }
    else {
      return Long.valueOf(value);
    }
  }

  @Override
  public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
    String jmsCorrelationID = getJMSCorrelationID();

    if (jmsCorrelationID != null) {
      try {
        return jmsCorrelationID.getBytes("UTF-8");
      }
      catch (UnsupportedEncodingException ex) {
        throw new JMSException("Unable to convert correlation ID to bytes: "
            + ex.getMessage());
      }
    }
    return null;
  }

  @Override
  public int getJMSDeliveryMode() throws JMSException {
    return getHeader(HEADER_JMS_DELIVERY_MODE, DeliveryMode.NON_PERSISTENT);
  }

  @Override
  public Destination getJMSDestination() throws JMSException {
    return getDestinationHeader(HEADER_JMS_DESTINATION);
  }

  @Override
  public long getJMSExpiration() throws JMSException {
    return getHeader(HEADER_JMS_EXPIRATION, 0L);
  }

  @Override
  public String getJMSMessageID() throws JMSException {
    return getHeader(HEADER_JMS_MESSAGE_ID, null);
  }

  @Override
  public int getJMSPriority() throws JMSException {
    return getHeader(HEADER_JMS_PRIORITY, 4);
  }

  @Override
  public boolean getJMSRedelivered() throws JMSException {
    return Boolean
        .valueOf(getHeader(HEADER_JMS_REDELIVERED, Boolean.FALSE.toString()));
  }

  @Override
  public Destination getJMSReplyTo() throws JMSException {
    return getDestinationHeader(HEADER_JMS_REPLY_TO);
  }

  @Override
  public long getJMSTimestamp() throws JMSException {
    return getHeader(HEADER_JMS_TIMESTAMP, 0L);
  }

  @Override
  public String getJMSType() throws JMSException {
    return getHeader(HEADER_JMS_TYPE, "TextMessage");
  }

  @Override
  public long getLongProperty(String name) throws JMSException {
    return Long.valueOf(getStringProperty(name));
  }

  @Override
  public Object getObjectProperty(String name) throws JMSException {
    return properties.get(name);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Enumeration getPropertyNames() throws JMSException {
    return Collections.enumeration(properties.keySet());
  }

  @Override
  public short getShortProperty(String name) throws JMSException {
    return Short.valueOf(getStringProperty(name));
  }

  @Override
  public String getStringProperty(String name) throws JMSException {
    return properties.get(name);
  }

  @Override
  public boolean propertyExists(String name) throws JMSException {
    return properties.containsKey(name);
  }

  @Override
  public void setBooleanProperty(String name, boolean value)
      throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  @Override
  public void setByteProperty(String name, byte value) throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  @Override
  public void setDoubleProperty(String name, double value) throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  @Override
  public void setFloatProperty(String name, float value) throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  @Override
  public void setIntProperty(String name, int value) throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  @Override
  public void setJMSCorrelationID(String correlationID) throws JMSException {
    headers.put(HEADER_JMS_CORRELATION_ID, correlationID);
  }

  @Override
  public void setJMSCorrelationIDAsBytes(byte[] correlationID)
      throws JMSException {
    try {
      setJMSCorrelationID(new String(correlationID, "UTF-8"));
    }
    catch (UnsupportedEncodingException ex) {
      throw new JMSException("Unable to convert bytes to correlation ID: "
          + ex.getMessage());
    }
  }

  @Override
  public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
    headers.put(HEADER_JMS_DELIVERY_MODE, String.valueOf(deliveryMode));
  }

  @Override
  public void setJMSDestination(Destination destination) throws JMSException {
    setDestinationHeader(destination, HEADER_JMS_DESTINATION);
  }

  /**
   * Sets the given header to the given destination while also setting an
   * internal header used to track the type of the destination.
   *
   * @param destination the destination value to set or null to clear it
   * @param headerName the name of the header to store the value in
   *
   * @throws JMSException if there is an error setting the header
   */
  private void setDestinationHeader(Destination destination, String headerName)
      throws JMSException {
    if (destination == null) {
      headers.remove(headerName);
      headers.remove(HEADER_PREFIX_DESTINATION_TYPE + headerName);
    }
    else {
      String dest;
      String destType;
      if (destination instanceof HazelcastMQJmsTemporaryQueue) {
        dest = ((HazelcastMQJmsQueue) destination).getQueueName();
        destType = "temporary-queue";
      }
      else if (destination instanceof HazelcastMQJmsQueue) {
        dest = ((HazelcastMQJmsQueue) destination).getQueueName();
        destType = "queue";
      }
      else if (destination instanceof HazelcastMQJmsTemporaryTopic) {
        dest = ((HazelcastMQJmsTopic) destination).getTopicName();
        destType = "temporary-topic";
      }
      else {
        dest = ((HazelcastMQJmsTopic) destination).getTopicName();
        destType = "topic";
      }
      headers.put(headerName, dest);
      headers.put(HEADER_PREFIX_DESTINATION_TYPE + headerName, destType);
    }
  }

  /**
   * Returns a header which represents a JMS destination. If the header exists,
   * it will be converted into the appropriate destination type and returned.
   * 
   * @param headerName
   *          the name of the header to read
   * @return the destination or null if the header is not defined
   * @throws JMSException
   */
  private Destination getDestinationHeader(String headerName)
      throws JMSException {
    String dest = getHeader(headerName, null);
    if (dest == null) {
      return null;
    }
    String destType = getHeader("HZ" + headerName + "Type", "queue");

    if (destType.equals("topic")) {
      return new HazelcastMQJmsTopic(dest);
    }
    else if (destType.equals("temporary-topic")) {
      return new HazelcastMQJmsTemporaryTopic(dest);
    }
    else if (destType.equals("temporary-queue")) {
      return new HazelcastMQJmsTemporaryQueue(dest);
    }
    else {
      return new HazelcastMQJmsQueue(dest);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSExpiration(long)
   */
  @Override
  public void setJMSExpiration(long expiration) throws JMSException {
    headers.put(HEADER_JMS_EXPIRATION, String.valueOf(expiration));
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSMessageID(java.lang.String)
   */
  @Override
  public void setJMSMessageID(String messageID) throws JMSException {
    headers.put(HEADER_JMS_MESSAGE_ID, messageID);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSPriority(int)
   */
  @Override
  public void setJMSPriority(int priority) throws JMSException {
    headers.put(HEADER_JMS_PRIORITY, String.valueOf(priority));
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSRedelivered(boolean)
   */
  @Override
  public void setJMSRedelivered(boolean redelivered) throws JMSException {
    headers.put(HEADER_JMS_REDELIVERED, Boolean.valueOf(redelivered).toString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSReplyTo(javax.jms.Destination)
   */
  @Override
  public void setJMSReplyTo(Destination destination) throws JMSException {
    setDestinationHeader(destination, HEADER_JMS_REPLY_TO);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSTimestamp(long)
   */
  @Override
  public void setJMSTimestamp(long timestamp) throws JMSException {
    headers.put(HEADER_JMS_TIMESTAMP, String.valueOf(timestamp));
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setJMSType(java.lang.String)
   */
  @Override
  public void setJMSType(String type) throws JMSException {
    headers.put(HEADER_JMS_TYPE, type);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setLongProperty(java.lang.String, long)
   */
  @Override
  public void setLongProperty(String name, long value) throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setObjectProperty(java.lang.String,
   * java.lang.Object)
   */
  @Override
  public void setObjectProperty(String name, Object value) throws JMSException {
    properties.put(name, value.toString());

  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setShortProperty(java.lang.String, short)
   */
  @Override
  public void setShortProperty(String name, short value) throws JMSException {
    properties.put(name, String.valueOf(value));
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.Message#setStringProperty(java.lang.String,
   * java.lang.String)
   */
  @Override
  public void setStringProperty(String name, String value) throws JMSException {
    properties.put(name, value);
  }

  /**
   * Returns all the defined JMS headers. The properties are backed by the
   * message so modifications will become part of the message.
   * 
   * @return the JMS headers or an empty set of properties if none are defined
   */
  Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * Returns all the user defined message properties. The properties are backed
   * by the message so modifications will become part of the message.
   * 
   * @return the user defined properties or an empty set of properties if none
   *         are defined
   */
  Map<String, String> getProperties() {
    return properties;
  }

}
