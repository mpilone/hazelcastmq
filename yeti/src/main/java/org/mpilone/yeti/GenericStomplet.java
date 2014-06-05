
package org.mpilone.yeti;

/**
 * A generic abstract implementation of a {@link Stomplet} that provides an
 * implementation of the basic methods.
 *
 * @author mpilone
 */
public abstract class GenericStomplet implements Stomplet {

  /**
   * The stomplet context.
   */
  private StompletContext stompletContext;

  @Override
  public void init(StompletContext context) {
    this.stompletContext = context;
  }

  /**
   * Returns the stomplet context set during initialization.
   *
   * @return the context
   */
  protected StompletContext getStompletContext() {
    return stompletContext;
  }

}
