package com.example.redis.config;

import com.redis.testcontainers.RedisContainer;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.utility.DockerImageName;

@Configuration
public class RedisConfig {

  private static final String REDIS_IMAGE_NAME = "redis:8.2.2-alpine";

  @Bean
  @ServiceConnection
  public RedisContainer redisContainer() {
    return new RedisContainer(DockerImageName.parse(REDIS_IMAGE_NAME))
        .withEnv("ALLOW_EMPTY_PASSWORD", "yes");
  }
}
