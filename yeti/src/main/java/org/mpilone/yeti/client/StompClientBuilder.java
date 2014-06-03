
package org.mpilone.yeti.client;

/**
 *
 * @author mpilone
 */
public class StompClientBuilder {

  private StompClient client;

  private StompClientBuilder() {
    this.client = new StompClient();
  }

  public StompClientBuilder host(String host) {
    client.setHost(host);
    return this;
  }

  public static StompClientBuilder port(int port) {
    StompClientBuilder ssb = new StompClientBuilder();
    ssb.client.setPort(port);

    return ssb;
  }

  public StompClientBuilder frameDebug(boolean enabled) {
    client.setFrameDebugEnabled(enabled);
    return this;
  }

  public StompClient build() {
    StompClient tmp = client;
    client = null;

    return tmp;
  }

}
