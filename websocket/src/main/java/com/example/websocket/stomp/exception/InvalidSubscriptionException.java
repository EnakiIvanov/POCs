package com.example.websocket.stomp.exception;

public class InvalidSubscriptionException extends RuntimeException {

  public static final String CODE = "INVALID_SUBSCRIPTION";

  public InvalidSubscriptionException() {
    super("You cannot subscribe to this room");
  }
}
