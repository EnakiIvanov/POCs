package com.example.websocket.stomp.model;

public record ChatMessage(MessageType type, String sender, String content){}