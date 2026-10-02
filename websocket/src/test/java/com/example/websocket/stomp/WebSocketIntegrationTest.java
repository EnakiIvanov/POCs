package com.example.websocket.stomp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.example.websocket.stomp.model.ChatMessage;
import com.example.websocket.stomp.model.MessageType;
import com.example.websocket.stomp.model.RoomState;
import com.example.websocket.stomp.model.WebSocketError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("stomp")
class WebSocketIntegrationTest {

  @LocalServerPort
  private int port;

  @Autowired
  private ObjectMapper objectMapper;

  private WebSocketStompClient stompClient;

  @Autowired
  private SessionRegistry sessionRegistry;

  private final List<StompSession> sessions = new ArrayList<>();

  @BeforeEach
  void setUp() {
    stompClient = new WebSocketStompClient(
        new StandardWebSocketClient()
    );

    stompClient.setMessageConverter(
        new MappingJackson2MessageConverter()
    );
  }

  @AfterEach
  void tearDown() {
    sessions.forEach(session -> {
      if (session.isConnected()) {
        session.disconnect();
      }
    });

    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> sessionRegistry.size() == 0);

    stompClient.stop();
    sessions.clear();
  }

  @Test
  void shouldConnectSubscribeAndReceiveMessage() throws Exception {
    var messages = new LinkedBlockingQueue<>();

    var session = connect("Alice", "General");

    session.subscribe("/topic/chat/General", createStompFrameHandler(messages));

    session.send("/app/chat", new ChatController.ChatRequest("Hello!"));

    var message = messages.poll(5, TimeUnit.SECONDS);
    assertRoomState(message, 1, "Alice");

    message = messages.poll(5, TimeUnit.SECONDS);
    assertChatMessage(message, MessageType.CHAT, "Alice", "Hello!");
  }

  @Test
  void shouldBroadcastMessageToAllUsersInRoom() throws Exception {
    var aliceMessages = new LinkedBlockingQueue<>();
    var bobMessages = new LinkedBlockingQueue<>();

    var alice = connect("Alice", "NonGeneral");
    alice.subscribe("/topic/chat/NonGeneral", createStompFrameHandler(aliceMessages));
    assertRoomState(aliceMessages.poll(5, TimeUnit.SECONDS), 1, "Alice");

    var bob = connect("Bob", "NonGeneral");
    bob.subscribe("/topic/chat/NonGeneral", createStompFrameHandler(bobMessages));
    assertRoomState(bobMessages.poll(5, TimeUnit.SECONDS), 2, "Alice", "Bob");

    assertChatMessage(aliceMessages.poll(5, TimeUnit.SECONDS),
        MessageType.SYSTEM, "SYSTEM", "Bob joined the room"
    );
    assertRoomState(aliceMessages.poll(5, TimeUnit.SECONDS), 2, "Alice", "Bob");

    alice.send("/app/chat", new ChatController.ChatRequest("Hello Bob!"));

    // Both users should receive Alice's message
    assertChatMessage(aliceMessages.poll(5, TimeUnit.SECONDS),
        MessageType.CHAT, "Alice", "Hello Bob!"
    );

    assertChatMessage(bobMessages.poll(5, TimeUnit.SECONDS),
        MessageType.CHAT, "Alice", "Hello Bob!"
    );
  }

  @Test
  void shouldNotReceiveMessagesFromAnotherRoom() throws Exception {
    var aliceMessages = new LinkedBlockingQueue<>();
    var bobMessages = new LinkedBlockingQueue<>();

    var alice = connect("Alice", "General");
    alice.subscribe("/topic/chat/General", createStompFrameHandler(aliceMessages));
    assertRoomState(aliceMessages.poll(5, TimeUnit.SECONDS), 1, "Alice");

    var bob = connect("Bob", "Programming");
    bob.subscribe("/topic/chat/Programming", createStompFrameHandler(bobMessages));
    assertRoomState(bobMessages.poll(5, TimeUnit.SECONDS), 1, "Bob");

    alice.send("/app/chat", new ChatController.ChatRequest("Hello Bob!"));

    assertChatMessage(aliceMessages.poll(5, TimeUnit.SECONDS),
        MessageType.CHAT, "Alice", "Hello Bob!"
    );

    assertThat(bobMessages.poll(1, TimeUnit.SECONDS)).isNull();
  }

  @Test
  void shouldRejectConnectionWhenRoomIsFull() throws Exception {
    var aliceMessages = new LinkedBlockingQueue<>();
    var bobMessages = new LinkedBlockingQueue<>();
    var errors = new LinkedBlockingQueue<WebSocketError>();

    var alice = connect("Alice", "General");
    alice.subscribe("/topic/chat/General", createStompFrameHandler(aliceMessages));
    assertRoomState(aliceMessages.poll(5, TimeUnit.SECONDS), 1, "Alice");

    var bob = connect("Bob", "General");
    bob.subscribe("/topic/chat/General", createStompFrameHandler(bobMessages));
    assertRoomState(bobMessages.poll(5, TimeUnit.SECONDS), 2, "Alice", "Bob");

    assertThatThrownBy(() -> connect("Charlie", "General", errors))
        .isInstanceOf(ExecutionException.class);
    var error = errors.poll(5, TimeUnit.SECONDS);

    assertThat(error).isNotNull();
    assertThat(error.code()).isEqualTo("ROOM_FULL");
    assertThat(error.message()).isEqualTo("Room is full");
  }

  @ParameterizedTest
  @MethodSource("invalidConnectionArguments")
  void shouldRejectInvalidConnectionHeaders(
      String username,
      String room,
      String expectedMessage
  ) throws Exception {
    var errors = new LinkedBlockingQueue<WebSocketError>();

    assertThatThrownBy(() -> connect(username, room, errors))
        .isInstanceOf(ExecutionException.class);

    var error = errors.poll(5, TimeUnit.SECONDS);

    assertThat(error).isNotNull();
    assertThat(error.code()).isEqualTo("INVALID_CONNECTION");
    assertThat(error.message()).isEqualTo(expectedMessage);
  }

  private static Stream<Arguments> invalidConnectionArguments() {
    return Stream.of(
        Arguments.of(null, "General", "Username is required"),
        Arguments.of("", "General", "Username is required"),
        Arguments.of("   ", "General", "Username is required"),
        Arguments.of("A".repeat(31), "General", "Username is too long"),

        Arguments.of("Alice", null, "Room is required"),
        Arguments.of("Alice", "", "Room is required"),
        Arguments.of("Alice", "   ", "Room is required"),
        Arguments.of("Alice", "A".repeat(101), "Room is too long")
    );
  }

  @Test
  void shouldRejectSubscriptionToAnotherRoom() throws Exception {
    var errors = new LinkedBlockingQueue<WebSocketError>();

    var alice = connect("Alice", "General", errors);

    alice.subscribe("/topic/chat/Programming",
        createStompFrameHandler(new LinkedBlockingQueue<>()));

    var error = errors.poll(5, TimeUnit.SECONDS);

    assertThat(error).isNotNull();
    assertThat(error.code()).isEqualTo("INVALID_SUBSCRIPTION");
    assertThat(error.message()).isEqualTo("You cannot subscribe to this room");
  }

  @Test
  void shouldUpdateRoomStateWhenUserDisconnects() throws Exception {
    var aliceMessages = new LinkedBlockingQueue<>();
    var bobMessages = new LinkedBlockingQueue<>();

    var alice = connect("Alice", "General");
    alice.subscribe("/topic/chat/General", createStompFrameHandler(aliceMessages));

    assertRoomState(aliceMessages.poll(5, TimeUnit.SECONDS), 1, "Alice");

    var bob = connect("Bob", "General");
    bob.subscribe("/topic/chat/General", createStompFrameHandler(bobMessages));

    assertRoomState(bobMessages.poll(5, TimeUnit.SECONDS),
        2, "Alice", "Bob");

    // Alice receives the notifications caused by Bob joining.
    assertChatMessage(aliceMessages.poll(5, TimeUnit.SECONDS),
        MessageType.SYSTEM, "SYSTEM", "Bob joined the room");

    assertRoomState(aliceMessages.poll(5, TimeUnit.SECONDS),
        2, "Alice", "Bob");

    alice.disconnect();

    assertChatMessage(bobMessages.poll(5, TimeUnit.SECONDS),
        MessageType.SYSTEM, "SYSTEM", "Alice left the room");

    assertRoomState(bobMessages.poll(5, TimeUnit.SECONDS), 1, "Bob");
  }

  private void assertChatMessage(Object message, MessageType type, String sender, String content) {
    assertThat(message).isInstanceOf(ChatMessage.class);

    var chatMessage = (ChatMessage) message;

    assertThat(chatMessage.type()).isEqualTo(type);
    assertThat(chatMessage.sender()).isEqualTo(sender);
    assertThat(chatMessage.content()).isEqualTo(content);
  }

  private void assertRoomState(Object message, int userCount, String... users) {
    assertThat(message).isInstanceOf(RoomState.class);
    var roomState = (RoomState) message;

    assertThat(roomState.type()).isEqualTo(MessageType.ROOM_STATE);
    assertThat(roomState.userCount()).isEqualTo(userCount);
    assertThat(roomState.users()).containsExactlyInAnyOrder(users);
  }

  private @NotNull StompFrameHandler createStompFrameHandler(LinkedBlockingQueue<Object> messages) {
    return new StompFrameHandler() {
      @Override
      public @NotNull Type getPayloadType(@NotNull StompHeaders headers) {
        return JsonNode.class;
      }

      @Override
      public void handleFrame(@NotNull StompHeaders headers, Object payload) {
        JsonNode node = (JsonNode) payload;

        var type = MessageType.valueOf(node.get("type").asText());

        var message = switch (type) {
          case CHAT, SYSTEM -> convertObject(node, ChatMessage.class);
          case ROOM_STATE -> convertObject(node, RoomState.class);
        };

        messages.add(message);
      }
    };
  }

  private <T> T convertObject(JsonNode node, Class<T> clazz) {
    try {
      return objectMapper.treeToValue(node, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private StompSession connect(String username, String room) throws Exception {
    var headers = new StompHeaders();
    headers.add("username", username);
    headers.add("room", room);

    var session =  stompClient.connectAsync("ws://localhost:" + port + "/ws",
            new WebSocketHttpHeaders(), headers,
            createStompSessionHandlerAdapter(new LinkedBlockingQueue<>()))
        .get(5, TimeUnit.SECONDS);

    sessions.add(session);

    return session;
  }

  private StompSession connect(String username, String room,
      LinkedBlockingQueue<WebSocketError> errors) throws Exception {
    var headers = new StompHeaders();

    if (username != null) {
      headers.add("username", username);
    }

    if (room != null) {
      headers.add("room", room);
    }

    var session = stompClient.connectAsync("ws://localhost:" + port + "/ws",
            new WebSocketHttpHeaders(), headers, createStompSessionHandlerAdapter(errors))
        .get(5, TimeUnit.SECONDS);

    sessions.add(session);

    return session;
  }

  private StompSessionHandlerAdapter createStompSessionHandlerAdapter(
      LinkedBlockingQueue<WebSocketError> errors) {
    return new StompSessionHandlerAdapter() {
      @Override
      public void handleException(
          @NotNull StompSession session,
          StompCommand command,
          @NotNull StompHeaders headers,
          byte @NotNull [] payload,
          @NotNull Throwable exception
      ) {
        errors.add(convertWebSocketError(payload));
      }

      @Override
      public void handleTransportError(
          @NotNull StompSession session,
          @NotNull Throwable exception
      ) {
        if (!(exception instanceof ConnectionLostException)) {
          exception.printStackTrace();
        }
      }

      @Override
      public void handleFrame(
          @NotNull StompHeaders headers,
          Object payload
      ) {
        System.out.println("ERROR FRAME: " + new String((byte[]) payload));
      }
    };
  }

  private WebSocketError convertWebSocketError(byte[] payload) {
    try {
      return objectMapper.readValue(payload, WebSocketError.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
