package org.mpilone.stomp.shared;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * Default implementation of STOMP frame headers.
 *
 * @author mpilone
 */
class DefaultHeaders extends HashMap<String, String> implements Headers,
    Serializable {

  /**
   * Serialization ID.
   */
  private static final long serialVersionUID = 1L;
 
  @Override
  public String get(String headerName) {
    return super.get(headerName);
  }

  @Override
  public Set<String> getHeaderNames() {
    return keySet();
  }
 
  @Override
  public void remove(String headerName) {
    super.remove(headerName);
  }
}
