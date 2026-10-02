# WebSocket POC

A small project demonstrating WebSocket communication with Spring Boot, both with raw WebSockets and STOMP.

## Raw WebSocket vs STOMP

The POC first demonstrates a raw WebSocket connection to show what WebSocket provides at the lowest level.

With a raw WebSocket, the server receives and sends messages directly:

```text
Browser ─────────────────── Spring
              WebSocket
```

The server-side handler manually processes messages, for example through `handleTextMessage(...)`.

### The problem with raw WebSockets

Imagine a real chat with Alice, Bob, and Charlie:

```text
Alice ────────┐
Bob ──────────┼── WebSocket Server
Charlie ──────┘
```

Now Alice sends:

```text
Hello!
```

The server has to figure out:

* Who should receive it?
* Should everyone receive it?
* Is it a private message?
* Which chat room does it belong to?
* Which sessions belong to that room?
* How should subscriptions be managed?

With raw WebSockets, we would have to build this messaging layer ourselves.

STOMP provides a messaging abstraction on top of WebSocket, giving us concepts such as destinations, subscriptions, commands, and message routing.

The rest of this POC demonstrates how Spring's STOMP support can be used to build that messaging layer.

## Why These Mechanisms?

### STOMP over WebSocket

WebSocket provides the persistent, bidirectional connection, while STOMP adds a messaging protocol on top of it.

STOMP was chosen because it provides destinations, subscriptions, commands, and message types without having to design a custom messaging protocol.

### SimpleBroker

Spring's in-memory `SimpleBroker` is used to route messages between connected clients.

It is sufficient for this POC because the application runs as a single instance and does not require a persistent or distributed message broker.

### [SessionRegistry](src/main/java/com/example/websocket/stomp/SessionRegistry.java)

`SessionRegistry` keeps track of active WebSocket sessions and their associated username and room.

It is used for room membership, presence information, room capacity checks, and disconnect cleanup.

An in-memory registry is enough here because the goal is to demonstrate session management rather than distributed state.

### [ChannelInterceptor](src/main/java/com/example/websocket/stomp/WebSocketChannelInterceptor.java)

`ChannelInterceptor` is used for validation at the STOMP message level.

It validates connection headers during `CONNECT` and verifies that clients only subscribe to the room they joined.

This keeps protocol-level validation outside the controllers.

### [Lifecycle Events](src/main/java/com/example/websocket/stomp/WebSocketSessionEventListener.java)

Spring's WebSocket/STOMP lifecycle events are used to react to connection, subscription, and disconnection events.

* `SessionConnectEvent` — register the new session.
* `SessionSubscribeEvent` — send the current room state after the client has subscribed.
* `SessionDisconnectEvent` — remove the session and update the remaining users.

This keeps connection lifecycle logic separate from message-handling controllers.

### SimpMessagingTemplate

`SimpMessagingTemplate` is used when the server needs to send a message independently of an incoming client message.

For example, it is used to broadcast chat messages, system messages, and room state updates to a specific room destination.

### [Separate Message Types](src/main/java/com/example/websocket/stomp/model/MessageType.java)

Messages use a `type` field (`CHAT`, `SYSTEM`, `ROOM_STATE`) so the client can distinguish different kinds of messages sent through the same destination.

Separate DTOs are used for the different payload structures instead of forcing unrelated messages into one generic model.

### [STOMP Error Handling](src/main/java/com/example/websocket/stomp/StompErrorHandler.java)

A custom `StompSubProtocolErrorHandler` converts known server-side exceptions into predictable STOMP `ERROR` frames.

The client receives a stable error code and message, for example:

```json
{
  "code": "ROOM_FULL",
  "message": "Room is full"
}
```

This keeps protocol-level errors separate from normal application messages.

### [@MessageExceptionHandler](src/main/java/com/example/websocket/stomp/WebSocketExceptionHandler.java)

`@MessageExceptionHandler` handles exceptions raised while processing application messages such as `/app/chat`.

This provides a simple way to return structured errors to the client without putting error-handling logic directly into controller methods.

### [Heartbeats](src/main/java/com/example/websocket/stomp/WebSocketConfig.java)

STOMP heartbeats are enabled to detect connections that are no longer alive even when no application messages are being exchanged.

A `TaskScheduler` is used by the broker to schedule heartbeat processing.

### [Integration Tests](src/test/java/com/example/websocket/stomp/WebSocketIntegrationTest.java)

The POC uses real WebSocket connections with `WebSocketStompClient` instead of mocking the WebSocket layer.

This verifies the complete flow:

```text
WebSocket
   ↓
STOMP
   ↓
Interceptor
   ↓
Controller / Broker
   ↓
STOMP response
   ↓
WebSocket client
```

The tests cover connection lifecycle, room isolation, broadcasting, room capacity, subscription validation, error handling, heartbeats, and disconnect cleanup.
