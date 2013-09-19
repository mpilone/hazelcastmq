package org.mpilone.hazelcastmq.core;

import java.util.HashMap;
import java.util.Set;

/**
 * @author mpilone
 * 
 */
class DefaultHeaders extends HashMap<String, String> implements Headers {

  /**
   * Serialization ID.
   */
  private static final long serialVersionUID = 1L;

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.Headers#get(java.lang.String)
   */
  @Override
  public String get(String headerName) {
    return super.get(headerName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.Headers#getHeaderNames()
   */
  @Override
  public Set<String> getHeaderNames() {
    return keySet();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.mpilone.hazelcastmq.core.Headers#remove(java.lang.String)
   */
  @Override
  public void remove(String headerName) {
    super.remove(headerName);
  }

}
