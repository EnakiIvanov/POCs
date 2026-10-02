package com.example.websocket.stomp;

import com.example.websocket.stomp.exception.InvalidConnectionException;
import com.example.websocket.stomp.exception.InvalidSubscriptionException;
import com.example.websocket.stomp.exception.RoomFullException;
import com.example.websocket.stomp.model.WebSocketError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.StompSubProtocolErrorHandler;

@Slf4j
@Component
@RequiredArgsConstructor
public class StompErrorHandler extends StompSubProtocolErrorHandler {

  private static final byte[] FALLBACK_ERROR =
      "{\"code\":\"INTERNAL_ERROR\",\"message\":\"Internal server error\"}"
          .getBytes(StandardCharsets.UTF_8);

  public final ObjectMapper objectMapper;

  @Override
  protected @NotNull Message<byte[]> handleInternal(
      @NotNull StompHeaderAccessor errorHeaderAccessor,
      byte @NotNull [] errorPayload, @Nullable Throwable cause,
      @Nullable StompHeaderAccessor clientHeaderAccessor) {

    while (cause != null) {
      if (cause instanceof RoomFullException exception) {
        return buildError(errorHeaderAccessor, RoomFullException.CODE,
            exception.getMessage());
      }

      if (cause instanceof InvalidConnectionException exception) {
        return buildError(errorHeaderAccessor, InvalidConnectionException.CODE,
            exception.getMessage());
      }

      if(cause instanceof InvalidSubscriptionException exception) {
        return buildError(errorHeaderAccessor, InvalidSubscriptionException.CODE,
            exception.getMessage());
      }

      cause = cause.getCause();
    }

    return super.handleInternal(errorHeaderAccessor, errorPayload, null, clientHeaderAccessor);
  }

  private Message<byte[]> buildError(StompHeaderAccessor errorHeaderAccessor, String code,
      String message) {
    errorHeaderAccessor.setMessage("Failed to process CONNECT");

    return MessageBuilder.createMessage(
        buildMessage(code, message),
        errorHeaderAccessor.getMessageHeaders()
    );
  }

  private byte[] buildMessage(String code, String message) {
    try {
      return objectMapper.writeValueAsBytes(new WebSocketError(code, message));
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize WebSocket error", e);
      return FALLBACK_ERROR;
    }
  }
}
