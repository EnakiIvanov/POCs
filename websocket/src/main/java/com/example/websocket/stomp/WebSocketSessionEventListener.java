package com.example.websocket.stomp;

import com.example.websocket.stomp.model.ChatMessage;
import com.example.websocket.stomp.model.ConnectionInfo;
import com.example.websocket.stomp.model.MessageType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.AbstractSubProtocolEvent;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketSessionEventListener {

  private final SessionRegistry sessionRegistry;
  private final SimpMessagingTemplate messagingTemplate;

  @EventListener
  public void handleSessionConnect(SessionConnectEvent event) {
    var connectionInfo = getConnectionInfo(event);
    sessionRegistry.register(connectionInfo);

    log.info("{} connected", connectionInfo.username());
    sendSystemMessage(connectionInfo, " joined the room");
  }

  @EventListener
  public void handleSessionDisconnect(SessionDisconnectEvent event) {
    log.info("DISCONNECT EVENT");
    var connectionInfo = getConnectionInfo(event);
    var removed = sessionRegistry.remove(connectionInfo.sessionId());

    if (removed != null) {
      log.info("{} disconnected", removed.username());
      sendSystemMessage(removed, " left the room");
      sendRoomState(removed);
    }
  }

  @EventListener
  public void handleSubscribeEvent(SessionSubscribeEvent event) {
    var sessionId = StompHeaderAccessor.wrap(event.getMessage()).getSessionId();
    var connectionInfo = sessionRegistry.get(sessionId);
    if (connectionInfo != null) {
      sendRoomState(connectionInfo);
    }
  }

  @EventListener
  public void handleUnsubscribeEvent(SessionUnsubscribeEvent event) {
    log.info("empty unsubscribe event");
  }

  private ConnectionInfo getConnectionInfo(AbstractSubProtocolEvent event) {
    var accessor = StompHeaderAccessor.wrap(event.getMessage());

    return ConnectionInfo.builder()
        .sessionId(accessor.getSessionId())
        .username(accessor.getFirstNativeHeader("username"))
        .room(accessor.getFirstNativeHeader("room"))
        .build();
  }

  private void sendSystemMessage(ConnectionInfo info, String message) {
    messagingTemplate.convertAndSend(info.getRoomDestination(),
        new ChatMessage(MessageType.SYSTEM, "SYSTEM", info.username() + message));
  }

  private void sendRoomState(ConnectionInfo info) {
    messagingTemplate.convertAndSend(info.getRoomDestination(),
        sessionRegistry.getRoomState(info.room())
    );
  }
}
