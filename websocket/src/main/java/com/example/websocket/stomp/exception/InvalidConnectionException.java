package com.example.websocket.stomp.exception;

public class InvalidConnectionException extends RuntimeException {

  public static final String CODE = "INVALID_CONNECTION";

  public InvalidConnectionException(String message) {
    super(message);
  }
}
