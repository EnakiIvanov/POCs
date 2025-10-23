package com.example.redis.stream;

import static com.example.redis.config.RedisStreamConfig.STREAM_NAME;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventProducer {

  private final StringRedisTemplate redisTemplate;

  public void sendEvent(String message) {
    Map<String, String> body = Map.of("Message", message);
    redisTemplate.opsForStream().add(STREAM_NAME, body);
  }
}
