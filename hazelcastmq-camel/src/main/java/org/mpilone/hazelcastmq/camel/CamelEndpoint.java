package org.mpilone.hazelcastmq.camel;


import java.util.concurrent.ExecutorService;

import org.apache.camel.*;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.ExecutorServiceManager;
import org.apache.camel.spi.ThreadPoolProfile;
import org.mpilone.hazelcastmq.core.DataStructureKey;
import org.mpilone.hazelcastmq.core.HazelcastMQ;


/**
 * <p>
 * An Apache Camel endpoint for creating consumers and producers on
 * {@link HazelcastMQ} channels.
 * </p>
 * <p>
 * Refer to the {@link CamelConfig} class for details on * configuration and
 * supported URL parameters.
 * </p>
 *
 * @author mpilone
 */
public class CamelEndpoint extends DefaultEndpoint implements
    MultipleConsumersSupport {

  private static final Object EXECUTOR_MUTEX = new Object();
  private static ExecutorService executorService;

  private final DataStructureKey channelKey;
  private final CamelConfig configuration;

  /**
   * Constructs the endpoint.
   *
   * @param uri the original endpoint URI
   * @param component the component that this endpoint belongs to
   * @param config the endpoint specific configuration
   * @param channelKey the HzMq message channel for all producers and *
   * consumers created by this endpoint
   */
  CamelEndpoint(String uri, CamelComponent component, CamelConfig config,
      String camelDestination) {
    super(uri, component);

    this.channelKey = DataStructureKey.fromString(camelDestination);
    this.configuration = config;
  }

  @Override
  protected void doStart() throws Exception {
    super.doStart();
  }

  @Override
  protected void doStop() throws Exception {
    super.doStop();
  }

  @Override
  protected void doShutdown() throws Exception {
    super.doShutdown();
  }

  @Override
  public Producer createProducer() throws Exception {
    return new CamelProducer(this);
  }

  @Override
  public boolean isMultipleConsumersSupported() {
    // Allow multiple consumers on all channels.
    return true;
  }

  @Override
  public Consumer createConsumer(Processor prcsr) throws Exception {
    Consumer c = new CamelConsumer(this, prcsr);
    configureConsumer(c);
    return c;
  }

  @Override
  public PollingConsumer createPollingConsumer() throws Exception {
    PollingConsumer pc = new CamelPollingConsumer(this);
    configurePollingConsumer(pc);
    return pc;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  /**
   * Returns the endpoint specific configuration.
   *
   * @return the configuration
   */
  CamelConfig getConfiguration() {
    return configuration;
  }

  /**
   * Returns the HzMq channel data structure key for all producers and consumers
   * created by this endpoint.
   *
   * @return the channel data structure key
   */
  DataStructureKey getChannelKey() {
    return channelKey;
  }

  private ExecutorService lookupExecutorService(CamelContext camelContext,
      String executorServiceRef) {

    final ExecutorServiceManager manager = camelContext.
        getExecutorServiceManager();
    ExecutorService executor = null;
    ThreadPoolProfile poolProfile = null;

    if (executorServiceRef != null) {

      // See if there is a registry entry for the executor directly.
      executor = camelContext.getRegistry().lookupByNameAndType(
          executorServiceRef, ExecutorService.class);

      // If not, see if there is a profile entry to create the pool.
      if (executor == null) {
        poolProfile = manager.getThreadPoolProfile(executorServiceRef);
      }
    }

    // If we don't have an executor, we need to create one.
    if (executor == null) {

      if (poolProfile == null) {
        poolProfile = manager.getDefaultThreadPoolProfile();
      }

      executor = manager.newThreadPool(this, getClass().getSimpleName(),
          poolProfile);
    }

    return executor;
  }

  ExecutorService getExecutorService() {

    synchronized (EXECUTOR_MUTEX) {

      // This code was taken from Facebook API producer and from the
      // now deprecated DefaultExecutorServiceStrategy.
      if (executorService == null || executorService.isTerminated()
          || executorService.isShutdown()) {

        executorService = lookupExecutorService(getCamelContext(),
            configuration.getExecutorServiceRef());
      }
    }

    return executorService;
  }
}
