package com.example.websocket.raw;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
@Component
@Profile("raw")
public class MyWebSocketHandler extends TextWebSocketHandler {

  @Override
  public void afterConnectionEstablished(WebSocketSession session) throws Exception {
    log.info("Client connected: {}", session.getId());

    session.sendMessage(new TextMessage("Welcome to the chat!"));
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
    log.info("Received: {}", message.getPayload());

    session.sendMessage(new TextMessage("Server received: " + message.getPayload()));
  }
}
