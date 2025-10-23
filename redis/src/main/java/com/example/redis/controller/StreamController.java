package com.example.redis.controller;

import com.example.redis.stream.EventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class StreamController {

  private final EventProducer producer;

  @PostMapping("/send/{msg}")
  public String send(@PathVariable String msg) {
    producer.sendEvent(msg);
    return "Sent: " + msg;
  }
}
