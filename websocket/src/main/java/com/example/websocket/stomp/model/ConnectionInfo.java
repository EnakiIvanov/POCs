package com.example.websocket.stomp.model;

import lombok.Builder;

@Builder
public record ConnectionInfo(
    String sessionId,
    String username,
    String room
) {

  public String getRoomDestination() {
    return "/topic/chat/" + room;
  }
}