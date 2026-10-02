package com.example.websocket.stomp;

import com.example.websocket.stomp.exception.InvalidConnectionException;
import org.springframework.stereotype.Component;

@Component
public class WebSocketConnectionValidator {

  private static final int MAX_USERNAME_LENGTH = 30;
  private static final int MAX_ROOM_LENGTH = 100;

  public void validate(String username, String room) {
    if (username == null || username.isBlank()) {
      throw new InvalidConnectionException("Username is required");
    }

    if (username.length() > MAX_USERNAME_LENGTH) {
      throw new InvalidConnectionException("Username is too long");
    }

    if (room == null || room.isBlank()) {
      throw new InvalidConnectionException("Room is required");
    }

    if (room.length() > MAX_ROOM_LENGTH) {
      throw new InvalidConnectionException("Room is too long");
    }
  }
}
