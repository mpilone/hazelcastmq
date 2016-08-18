package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.io.Serializable;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hazelcast.collection.impl.queue.QueueService;

/**
 * A key to lookup a Hazelcast data structure. The service name is normally
 * defined as a constant in the service implementation such as
 * {@link QueueService#SERVICE_NAME}.
 *
 * @author mpilone
 */
public class DataStructureKey implements Serializable,
    Comparable<DataStructureKey> {

  private static final long serialVersionUID = 1L;

  /**
   * The pattern to parse channel addresses. Supports both : separator and /
   * separator.
   */
  private static final Pattern CHANNEL_ADDRESS_PATTERN = Pattern.compile(
      "^[/:]?(queue)[/:](.*)");

  private final String name;
  private final String serviceName;

  /**
   * Constructs the key with the given data structure name and service name.
   *
   * @param name the name of the data structure
   * @param serviceName the service name/data structure type
   */
  public DataStructureKey(String name, String serviceName) {
    this.name = name;
    this.serviceName = serviceName;
  }

  /**
   * Returns the name of the data structure such as "foo.bar".
   *
   * @return the name of the data structure
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the name of the data structure service (i.e. type) such as
   * {@link QueueService#SERVICE_NAME}.
   *
   * @return the name of the service
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Converts a channel address to a HazelcastMQ {@link DataStructureKey}. For
   * example, a destination of {@code queue:foo.bar} will be converted to
   * service name {@link QueueService#SERVICE_NAME} with queue name
   * {@code foo.bar}. If a service name is not specified, a queue will be used
   * by default. Both {@code :} and {@code /} characters can be used such as
   * {@code /queue/foo.bar} and {@code queue:foo.bar}.
   *
   * @param channelAddress the address string to parse into a key
   *
   * @return the HazelcastMQ data structure key
   */
  public static DataStructureKey fromString(String channelAddress) {

    final Matcher matcher = CHANNEL_ADDRESS_PATTERN.matcher(channelAddress);
    if (matcher.matches()) {

      String serviceName = matcher.group(1);
      switch (serviceName) {
        case "queue":
          serviceName = QueueService.SERVICE_NAME;
          break;

        default:
          throw new IllegalArgumentException(format("Service name %s "
              + "in data structure address %s is not supported.", serviceName,
              channelAddress));
      }

      return new DataStructureKey(matcher.group(2), serviceName);
    }
    else {
      return new DataStructureKey(channelAddress,
          QueueService.SERVICE_NAME);
    }
  }

  @Override
  public int compareTo(DataStructureKey o) {
    return Comparator.comparing(DataStructureKey::getServiceName).thenComparing(
        DataStructureKey::getName).compare(this, o);
  }

}
