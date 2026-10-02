package com.example.websocket.stomp.exception;

public class RoomFullException extends RuntimeException {

  public static final String CODE = "ROOM_FULL";

  public RoomFullException() {
    super("Room is full");
  }
}
