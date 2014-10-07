package org.mpilone.hazelcastmq.core;

/**
 * The main HazelcastMQ instance which is thread-safe and can be used to create
 * multiple messaging contexts. Each individual context is NOT thread safe.
 * Refer to the {@link HazelcastMQContext} documentation for more details.
 * 
 * @author mpilone
 */
public interface HazelcastMQInstance {

  /**
   * Performs an orderly shutdown of the instance including all open contexts.
   * This method will not return until all the contexts have been closed.
   */
  public void shutdown();

  /**
   * Returns the configuration used to create this instance. The configuration
   * should not be modified after the instance is created.
   * 
   * @return the configuration used to create this instance
   */
  public HazelcastMQConfig getConfig();

  /**
   * Returns a new, non-transactional messaging context. The context can then be
   * used to create producers and consumers. All contexts created should be
   * closed to prevent resource leaks.
   * 
   * @return the newly created context
   */
  public HazelcastMQContext createContext();

  /**
   * Returns a new, optional transactional messaging context. The context can
   * then be used to create producers and consumers. All contexts created should
   * be closed to prevent resource leaks.
   * 
   * @param transacted
   *          true if the context will be transactional
   * @return the newly created context
   */
  public HazelcastMQContext createContext(boolean transacted);

  /**
   * Returns a new, transactional messaging context that can participate in
   * two-phase commit global transactions. The context can then be used to
   * create producers and consumers. All contexts created should be closed to
   * prevent resource leaks.
   *
   * @return the newly created context
   */
  public XAHazelcastMQContext createXAContext();

}
