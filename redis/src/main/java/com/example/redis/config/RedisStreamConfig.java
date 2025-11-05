package com.example.redis.config;

import com.example.redis.stream.EventConsumer;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfig {

  public static final String STREAM_NAME = "demo-stream";
  public static final String GROUP_NAME = "demo-group";

  private final StringRedisTemplate redisTemplate;

  /**
   * Creates a consumer group in Redis for the specified stream.
   * The group must be created first so Redis recognizes it;
   * otherwise, consumer group operations (e.g. XREADGROUP) will fail.
   *
   * STREAM_NAME the name of the stream to be consumed
   * GROUP_NAME  the name of the consumer group
   */
  @PostConstruct
  public void init() {
    //redisTemplate.opsForStream().createGroup(STREAM_NAME, GROUP_NAME);
  }

  @Bean
  public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamContainer(
      RedisConnectionFactory factory, EventConsumer consumer) {

    //Configures how the listener container should behave
    var options = StreamMessageListenerContainer
        .StreamMessageListenerContainerOptions
        .builder()
        .pollTimeout(Duration.ofSeconds(1)) //how long it waits when polling Redis for new events before trying again
        .build();

    //Creates the actual background container that knows how to connect to Redis and poll for stream entries
    var container = StreamMessageListenerContainer.create(factory, options);

    //Subscribes a consumer to the stream.
    //StreamOffset.latest(STREAM_KEY) means: “Only listen for new entries/events added after now.”
    container.receive(StreamOffset.latest(STREAM_NAME), consumer);

    //Starts the listener in the background
    container.start();

    //Return the container as a @Bean.
    //Makes it part of the Spring context, so it’s started automatically with the app
    return container;
  }
}
