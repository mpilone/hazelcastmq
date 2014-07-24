
package org.mpilone.hazelcastmq.example.spring.tx.support.hzmq;

import org.mpilone.hazelcastmq.core.HazelcastMQContext;
import org.mpilone.hazelcastmq.core.HazelcastMQInstance;
import org.mpilone.hazelcastmq.example.spring.tx.support.hzmq.BusinessService;
import org.mpilone.hazelcastmq.spring.tx.HazelcastMQUtils;
import org.mpilone.hazelcastmq.spring.tx.HazelcastUtils;


/**
 * A business service that uses {@link HazelcastUtils} to get transactional
 * instances of distributed objects when appropriate.
 *
 * @author mpilone
 */
public class BusinessServiceUsingUtils extends BusinessService {

  public BusinessServiceUsingUtils() {
  }

  public BusinessServiceUsingUtils(HazelcastMQInstance hazelcastMQInstance) {
    super(hazelcastMQInstance);
  }

  @Override
  protected HazelcastMQContext getContext(
      HazelcastMQInstance hazelcastMQInstance) {

    HazelcastMQContext context = HazelcastMQUtils.
        getTransactionalHazelcastMQContext(hazelcastMQInstance, true);

    if (context == null) {
      context = hazelcastMQInstance.createContext();
    }

    return context;
  }


}
