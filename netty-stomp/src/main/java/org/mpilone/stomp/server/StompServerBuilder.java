
package org.mpilone.stomp.server;

import org.mpilone.stomp.Stomplet;

/**
 *
 * @author mpilone
 */
public class StompServerBuilder {

  private StompServer server;

  private StompServerBuilder() {
    this.server = new StompServer();
  }

  public static StompServerBuilder port(int port) {
    StompServerBuilder ssb = new StompServerBuilder();
    ssb.server.setPort(port);

    return ssb;
  }

  public StompServerBuilder frameDebug(boolean enabled) {
    server.setFrameDebugEnabled(enabled);
    return this;
  }

  public StompServerBuilder stompletClass(
      final Class<? extends Stomplet> stompletClass) {
    stompletFactory(new StompServer.StompletFactory() {
      @Override
      public Stomplet createStomplet() throws Exception {
        return stompletClass.newInstance();
      }
    });

    return this;
  }

  public StompServerBuilder stompletFactory(StompServer.StompletFactory factory) {
    server.setStompletFactory(factory);
    return this;
  }

  public StompServer build() {
    StompServer tmp = server;
    server = null;

    return tmp;
  }

}
