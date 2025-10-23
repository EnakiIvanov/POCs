package com.example.redis.model;

import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@Data
@Builder
@RedisHash(value = "users", timeToLive = 5)//expire after 5 seconds
public class User {

  @Id
  private String id;
  private String name;
  private int age;
  private LocalDateTime updated;
}