package org.mpilone.hazelcastmq.jms;

import java.io.*;

import javax.jms.*;

/**
 * @author mpilone
 * 
 */
class HazelcastMQJmsBytesMessage extends HazelcastMQJmsMessage implements
    BytesMessage {

  private ByteArrayOutputStream outBuffer;
  private DataOutputStream outstream;
  private DataInputStream instream;
  private long length;

  public HazelcastMQJmsBytesMessage() throws JMSException {
    super();

    setJMSType("BytesMessage");

    outBuffer = new ByteArrayOutputStream();
    outstream = new DataOutputStream(outBuffer);
    length = 0;
  }

  private void checkReadMode() throws MessageNotReadableException {
    if (instream == null) {
      throw new MessageNotReadableException("Message is not in read mode.");
    }
  }

  private void checkWriteMode() throws MessageNotWriteableException {
    if (outstream == null) {
      throw new MessageNotWriteableException("Message is not in write mode.");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#getBodyLength()
   */
  @Override
  public long getBodyLength() throws JMSException {
    checkReadMode();
    return length;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readBoolean()
   */
  @Override
  public boolean readBoolean() throws JMSException {
    checkReadMode();

    try {
      return instream.readBoolean();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readByte()
   */
  @Override
  public byte readByte() throws JMSException {
    checkReadMode();

    try {
      return instream.readByte();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readBytes(byte[])
   */
  @Override
  public int readBytes(byte[] value) throws JMSException {
    checkReadMode();

    try {
      return instream.read(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readBytes(byte[], int)
   */
  @Override
  public int readBytes(byte[] value, int length) throws JMSException {
    checkReadMode();

    try {
      return instream.read(value, 0, length);
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readChar()
   */
  @Override
  public char readChar() throws JMSException {
    checkReadMode();

    try {
      return instream.readChar();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readDouble()
   */
  @Override
  public double readDouble() throws JMSException {
    checkReadMode();

    try {
      return instream.readDouble();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readFloat()
   */
  @Override
  public float readFloat() throws JMSException {
    checkReadMode();

    try {
      return instream.readFloat();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readInt()
   */
  @Override
  public int readInt() throws JMSException {
    checkReadMode();

    try {
      return instream.readInt();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readLong()
   */
  @Override
  public long readLong() throws JMSException {
    checkReadMode();

    try {
      return instream.readLong();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readShort()
   */
  @Override
  public short readShort() throws JMSException {
    checkReadMode();

    try {
      return instream.readShort();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readUTF()
   */
  @Override
  public String readUTF() throws JMSException {
    checkReadMode();

    try {
      return instream.readUTF();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readUnsignedByte()
   */
  @Override
  public int readUnsignedByte() throws JMSException {
    checkReadMode();

    try {
      return instream.readUnsignedByte();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#readUnsignedShort()
   */
  @Override
  public int readUnsignedShort() throws JMSException {
    checkReadMode();

    try {
      return instream.readUnsignedShort();
    }
    catch (IOException ex) {
      throw new JMSException("Error reading value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#reset()
   */
  @Override
  public void reset() throws JMSException {
    checkWriteMode();

    try {
      outstream.close();
      byte[] data = outBuffer.toByteArray();
      outstream = null;
      outBuffer = null;

      length = data.length;
      instream = new DataInputStream(new ByteArrayInputStream(data));
    }
    catch (IOException ex) {
      throw new JMSException("Error resetting into read mode: "
          + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeBoolean(boolean)
   */
  @Override
  public void writeBoolean(boolean value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeBoolean(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeByte(byte)
   */
  @Override
  public void writeByte(byte value) throws JMSException {
    checkWriteMode();

    try {
      outstream.write(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeBytes(byte[])
   */
  @Override
  public void writeBytes(byte[] value) throws JMSException {
    checkWriteMode();

    try {
      outstream.write(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeBytes(byte[], int, int)
   */
  @Override
  public void writeBytes(byte[] value, int offset, int length)
      throws JMSException {
    checkWriteMode();

    try {
      outstream.write(value, offset, length);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeChar(char)
   */
  @Override
  public void writeChar(char value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeChar(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeDouble(double)
   */
  @Override
  public void writeDouble(double value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeDouble(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeFloat(float)
   */
  @Override
  public void writeFloat(float value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeFloat(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeInt(int)
   */
  @Override
  public void writeInt(int value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeInt(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeLong(long)
   */
  @Override
  public void writeLong(long value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeLong(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeObject(java.lang.Object)
   */
  @Override
  public void writeObject(Object value) throws JMSException {
    checkWriteMode();

    if (value instanceof Boolean) {
      writeBoolean(((Boolean) value).booleanValue());
    }
    else if (value instanceof Character) {
      writeChar(((Character) value).charValue());
    }
    else if (value instanceof Byte) {
      writeByte(((Byte) value).byteValue());
    }
    else if (value instanceof Short) {
      writeShort(((Short) value).shortValue());
    }
    else if (value instanceof Integer) {
      writeInt(((Integer) value).intValue());
    }
    else if (value instanceof Long) {
      writeLong(((Long) value).longValue());
    }
    else if (value instanceof Float) {
      writeFloat(((Float) value).floatValue());
    }
    else if (value instanceof Double) {
      writeDouble(((Double) value).doubleValue());
    }
    else if (value instanceof String) {
      writeUTF(value.toString());
    }
    else if (value instanceof byte[]) {
      writeBytes((byte[]) value);
    }
    else {
      throw new MessageFormatException("Cannot write non-primitive type: "
          + value.getClass());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeShort(short)
   */
  @Override
  public void writeShort(short value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeShort(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.jms.BytesMessage#writeUTF(java.lang.String)
   */
  @Override
  public void writeUTF(String value) throws JMSException {
    checkWriteMode();

    try {
      outstream.writeUTF(value);
    }
    catch (IOException ex) {
      throw new JMSException("Error writing value: " + ex.getMessage());
    }
  }

}
