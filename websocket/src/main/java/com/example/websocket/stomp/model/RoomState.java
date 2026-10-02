package com.example.websocket.stomp.model;

import java.util.List;

public record RoomState(MessageType type, int userCount, List<String> users) {

}
