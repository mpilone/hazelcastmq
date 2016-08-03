package org.mpilone.hazelcastmq.core;

import static java.lang.String.format;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hazelcast.collection.impl.queue.QueueService;

/**
 *
 * @author mpilone
 */
public class DataStructureKey implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The pattern to parse channel addresses. Supports both : separator and /
   * separator.
   */
  private static final Pattern CHANNEL_ADDRESS_PATTERN = Pattern.compile(
      "^[/:]?(queue)[/:](.*)");

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

}
