package com.example.websocket.stomp;

import com.example.websocket.stomp.exception.SessionNotRegisteredException;
import com.example.websocket.stomp.model.ChatMessage;
import com.example.websocket.stomp.model.MessageType;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Controller
@RequiredArgsConstructor
public class ChatController {

  private final SessionRegistry sessionRegistry;
  private final SimpMessagingTemplate messagingTemplate;

  @MessageMapping("/chat")
  public void chat(ChatRequest request, SimpMessageHeaderAccessor accessor) {
    var connection = sessionRegistry.get(accessor.getSessionId());

    if (connection == null) {
      throw new SessionNotRegisteredException();
    }

    var message = new ChatMessage(
        MessageType.CHAT,
        connection.username(),
        request.content()
    );

    messagingTemplate.convertAndSend(connection.getRoomDestination(), message);
  }

  public record ChatRequest(String content) {

  }
}

