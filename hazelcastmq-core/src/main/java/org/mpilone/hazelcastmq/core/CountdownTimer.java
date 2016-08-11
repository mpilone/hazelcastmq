
package org.mpilone.hazelcastmq.core;

import java.util.concurrent.TimeUnit;

/**
 * A utility class that acts as a countdown timer. The timer can be started and
 * stopped multiple times and can return the amount of time remaining.
 *
 * @author mpilone
 */
class CountdownTimer {

  private final long timeout;
  private long remaining;
  private long start;

  /**
   * Constructs the timer with the given timeout.
   *
   * @param timeout the timeout value
   * @param unit the unit of the timeout value
   */
  public CountdownTimer(long timeout, TimeUnit unit) {
    this.timeout = this.remaining = unit.toMillis(timeout);
  }

  /**
   * Stops and resets the timer to the original timeout value.
   */
  public void reset() {
    stop();
    this.remaining = timeout;
  }

  /**
   * Starts the timer. If the timer is already started, this method does
   * nothing.
   */
  public void start() {
    if (start != 0) {
      return;
    }

    this.start = System.currentTimeMillis();
  }

  /**
   * Stops the timer. If the timer is already stopped, this method does nothing.
   */
  public void stop() {
    if (start == 0) {
      return;
    }

    // Reset the remaining value.
    this.remaining = getRemaining();
    this.start = 0;
  }

  /**
   * Returns the remaining time in milliseconds.
   *
   * @return the remaining time
   */
  public long getRemaining() {

    long elapsed = 0;
    if (start != 0) {
      elapsed = System.currentTimeMillis() - start;
    }

    return Math.max(0, remaining - elapsed);
  }

  /**
   * Returns the minimum of the remaining time or the given interval.
   *
   * @param interval the interval value
   * @param unit the unit of the interval
   *
   * @return the remaining time or interval in milliseconds
   */
  public long getRemainingOrInterval(long interval, TimeUnit unit) {
    return Math.min(getRemaining(), unit.toMillis(interval));
  }

  /**
   * Returns true if the timer has expired.
   *
   * @return true if expired
   */
  public boolean isExpired() {
    return getRemaining() <= 0;
  }

}
