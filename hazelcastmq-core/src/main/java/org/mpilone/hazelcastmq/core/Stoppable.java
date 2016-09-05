/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mpilone.hazelcastmq.core;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author mpilone
 */
public interface Stoppable {

  /**
   * Stops the target callable if it was already started and awaits the
   * termination of this task. This method will block until the task is executed
   * and completes.
   *
   * @throws InterruptedException if interrupted while awaiting termination
   */
  void stop() throws InterruptedException;

  /**
   * Stops the target callable if it was already started and awaits the
   * termination of this task. This method will block until the task is executed
   * and completes.
   *
   * @param timeout the amount of time to wait for termination
   * @param unit the unit of the time value
   *
   * @return true if the task terminated in the given time, false otherwise
   * @throws InterruptedException if interrupted while awaiting termination
   */
  public boolean stop(long timeout, TimeUnit unit) throws InterruptedException;
}
