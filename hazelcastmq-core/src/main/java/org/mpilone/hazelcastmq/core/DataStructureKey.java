/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mpilone.hazelcastmq.core;

import java.io.Serializable;

/**
 *
 * @author mpilone
 */
public class DataStructureKey implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String name;
  private final String serviceName;

  public DataStructureKey(String name, String serviceName) {
    this.name = name;
    this.serviceName = serviceName;
  }

  public String getName() {
    return name;
  }

  public String getServiceName() {
    return serviceName;
  }

}
