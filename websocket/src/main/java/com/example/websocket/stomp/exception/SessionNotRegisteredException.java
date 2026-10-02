package com.example.websocket.stomp.exception;

public class SessionNotRegisteredException extends RuntimeException {

  public static final String CODE = "SESSION_NOT_REGISTERED";

  public SessionNotRegisteredException() {
    super("Session is not registered");
  }
}
