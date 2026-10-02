package com.example.websocket.stomp;

import com.example.websocket.stomp.exception.InvalidSubscriptionException;
import com.example.websocket.stomp.exception.RoomFullException;
import com.example.websocket.stomp.model.ConnectionInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketChannelInterceptor implements ChannelInterceptor {

  private static final int MAX_USERS_PER_ROOM = 2;

  private final SessionRegistry sessionRegistry;
  private final WebSocketConnectionValidator validator;

  @Override
  public @Nullable Message<?> preSend(@NotNull Message<?> message, @NotNull MessageChannel channel) {
    var accessor = StompHeaderAccessor.wrap(message);

    if (!isSubscriptionAllowed(accessor)) {
      throw new InvalidSubscriptionException();
    }

    if (StompCommand.CONNECT.equals(accessor.getCommand())) {
      String room = accessor.getFirstNativeHeader("room");
      String username = accessor.getFirstNativeHeader("username");

      validator.validate(username, room);

      if (sessionRegistry.countUsersInRoom(room) >= MAX_USERS_PER_ROOM) {
        log.info("Room {} is full", room);
        throw new RoomFullException();
      }
    }

    return message;
  }

  private boolean isSubscriptionAllowed(StompHeaderAccessor accessor) {
    if (!StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
      return true;
    }

    ConnectionInfo connection = sessionRegistry.get(accessor.getSessionId());
    String destination = accessor.getDestination();

    if (connection == null || destination == null) {
      return false;
    }

    if (!destination.equals(connection.getRoomDestination())) {
      log.warn("Session {} attempted to subscribe to {} but belongs to {}",
          accessor.getSessionId(), destination, connection.getRoomDestination());

      return false;
    }

    return true;
  }

}
