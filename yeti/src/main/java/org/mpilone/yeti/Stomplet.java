/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mpilone.yeti;

/**
 *
 * @author mpilone
 */
public interface Stomplet {
  void init(StompletContext context);

  void service(StompletRequest req, StompletResponse res) throws Exception;

  void destroy();

  interface StompletRequest {
    public Frame getFrame();
  }

  interface StompletResponse {

    public WritableFrameChannel getFrameChannel();

    public void setFinalResponse(boolean finalResponse);
  }

  interface WritableFrameChannel {

    void write(Frame frame);
  }

  interface StompletContext {

  }
}
