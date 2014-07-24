package org.mpilone.hazelcastmq.spring.tx;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.*;

import org.junit.*;
import org.mockito.*;
import org.mpilone.hazelcastmq.core.*;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.*;

/**
 * Test case for the {@link HazelcastMQTransactionManager}.
 *
 * @author mpilone
 */
public class HazelcastMQTransactionManagerTestCase {

  @Mock
  private HazelcastMQInstance hzInstance;

  @Mock
  private HazelcastMQContext hzContext;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Tests creating a transaction which will begin the transaction and
   * synchronize the correct resources.
   */
  @Test
  public void testBegin() {

    // Given
    HazelcastMQTransactionManager txManager = new HazelcastMQTransactionManager(
        hzInstance);

    TransactionDefinition txDef = new DefaultTransactionDefinition();

    given(hzInstance.createContext(true)).willReturn(hzContext);

    // When
    txManager.getTransaction(txDef);

    // Then
    verify(hzInstance).createContext(true);
    verifyZeroInteractions(hzContext);

    HazelcastMQContextHolder holder =
        (HazelcastMQContextHolder) TransactionSynchronizationManager.
        getResource(hzInstance);
    assertNotNull(holder);
    assertTrue(holder.isTransactionActive());
    assertNotNull(holder.getHazelcastMQContext());
  }

  /**
   * Tests committing a transaction which should commit the context and cleanup
   * any resources.
   */
  @Test
  public void testCommit() {

    // Given
    HazelcastMQTransactionManager txManager = new HazelcastMQTransactionManager(
        hzInstance);

    TransactionDefinition txDef = new DefaultTransactionDefinition();

    given(hzInstance.createContext(true)).willReturn(hzContext);

    // When
    TransactionStatus txStatus = txManager.getTransaction(txDef);
    txManager.commit(txStatus);

    // Then
    verify(hzInstance).createContext(true);
    verify(hzContext).commit();
    verify(hzContext).close();

    HazelcastMQContextHolder holder =
        (HazelcastMQContextHolder) TransactionSynchronizationManager.
        getResource(hzInstance);
    assertNull(holder);
  }

  /**
   * Tests rolling back a transaction which should rollback the context and
   * cleanup any resources.
   */
  @Test
  public void testRollback() {
    // Given
    HazelcastMQTransactionManager txManager = new HazelcastMQTransactionManager(
        hzInstance);

    TransactionDefinition txDef = new DefaultTransactionDefinition();

    given(hzInstance.createContext(true)).willReturn(hzContext);

    // When
    TransactionStatus txStatus = txManager.getTransaction(txDef);
    txManager.rollback(txStatus);

    // Then
    verify(hzInstance).createContext(true);
    verify(hzContext).rollback();
    verify(hzContext).close();

    HazelcastMQContextHolder holder =
        (HazelcastMQContextHolder) TransactionSynchronizationManager.
        getResource(hzInstance);
    assertNull(holder);
  }

}
