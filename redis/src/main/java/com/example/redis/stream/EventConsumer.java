package com.example.redis.stream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EventConsumer implements StreamListener<String, MapRecord<String, String, String>> {

  @Override
  public void onMessage(MapRecord<String, String, String> message) {
    log.info("Received: {}", message.getValue());
  }
}
