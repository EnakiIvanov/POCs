package com.example.websocket.stomp;

import com.example.websocket.stomp.exception.SessionNotRegisteredException;
import com.example.websocket.stomp.model.WebSocketError;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.web.bind.annotation.ControllerAdvice;

@ControllerAdvice
public class WebSocketExceptionHandler {

  @MessageExceptionHandler(SessionNotRegisteredException.class)
  public WebSocketError handleSessionNotRegistered(SessionNotRegisteredException exception) {
    return new WebSocketError(SessionNotRegisteredException.CODE, exception.getMessage());
  }

}
