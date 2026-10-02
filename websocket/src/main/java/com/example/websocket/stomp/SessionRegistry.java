package com.example.websocket.stomp;

import com.example.websocket.stomp.model.ConnectionInfo;
import com.example.websocket.stomp.model.MessageType;
import com.example.websocket.stomp.model.RoomState;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SessionRegistry {

  private final Map<String, ConnectionInfo> sessions = new ConcurrentHashMap<>();

  public void register(ConnectionInfo info) {
    sessions.put(info.sessionId(), info);
  }

  public ConnectionInfo remove(String sessionId) {
    return sessions.remove(sessionId);
  }

  public ConnectionInfo get(String sessionId) {
    return sessions.get(sessionId);
  }

  public long countUsersInRoom(String room) {
    return sessions.values().stream()
        .filter(session -> room.equals(session.room()))
        .count();
  }

  public RoomState getRoomState(String room) {
    var users = getUsersInRoom(room);

    return new RoomState(MessageType.ROOM_STATE, users.size(), users);
  }

  public int size() {
    return sessions.size();
  }

  private List<String> getUsersInRoom(String room) {
    return sessions.values().stream()
        .filter(session -> session.room() != null && session.room().equals(room))
        .map(ConnectionInfo::username)
        .toList();
  }
}