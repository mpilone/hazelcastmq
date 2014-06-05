package org.mpilone.hazelcastmq.core;

import java.io.Serializable;
import java.util.*;

/**
 * Default implementation of HazelcastMQ message headers.
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
  public Collection<String> getHeaderNames() {
    return keySet();
  }

  @Override
  public Map<String, String> getHeaderMap() {
    return Collections.unmodifiableMap(this);
  }

  @Override
  public void remove(String headerName) {
    super.remove(headerName);
  }

}
