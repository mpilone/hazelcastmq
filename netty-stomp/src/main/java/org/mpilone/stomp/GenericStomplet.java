
package org.mpilone.stomp;

/**
 *
 * @author mpilone
 */
public abstract class GenericStomplet implements Stomplet {

  private StompletContext stompletContext;

  @Override
  public void init(StompletContext context) {
    this.stompletContext = context;
  }

  protected StompletContext getStompletContext() {
    return stompletContext;
  }

}
