package com.example.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.redis.config.RedisConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

@DataRedisTest
@Import(RedisConfig.class)
class RedisTemplateTest {

  @Autowired
  private StringRedisTemplate redisTemplate;

  @Test
  void string_simple_key_value() {
    String expectedValue = "1";
    redisTemplate.opsForValue().set("product:1", expectedValue);
    String actualValue = redisTemplate.opsForValue().get("product:1");

    assertThat(actualValue)
        .isEqualTo(expectedValue);
  }

  @Test
  void hash_objects_with_fields() {
    String expectedEmail = "alice@example.com";
    Map<String, String> user = new HashMap<>();
    user.put("name", "Alice");
    user.put("email", expectedEmail);

    redisTemplate.opsForHash().putAll("user:1", user);
    String actualEmail = (String) redisTemplate.opsForHash().get("user:1", "email");

    assertThat(actualEmail)
        .isEqualTo(expectedEmail);
  }

  @Test
  void list_ordered_collection() {
    redisTemplate.delete("tasks");
    redisTemplate.opsForList().leftPush("tasks", "task1");  // beginning
    redisTemplate.opsForList().rightPush("tasks", "task2"); // end

    List<String> tasks = redisTemplate.opsForList().range("tasks", 0, -1);

    assertThat(tasks)
        .containsExactly("task1", "task2");
  }

  @Test
  void set_unique_unordered_collection() {
    redisTemplate.delete("onlineUsers");
    redisTemplate.opsForSet().add("onlineUsers", "user1");
    redisTemplate.opsForSet().add("onlineUsers", "user2");
    redisTemplate.opsForSet().add("onlineUsers", "user2"); // duplicate ignored

    Set<String> users = redisTemplate.opsForSet().members("onlineUsers");

    assertThat(users)
        .hasSize(2)
        .containsExactlyInAnyOrder("user1", "user2");
  }

  @Test
  void sorted_set_ordered_by_score() {
    redisTemplate.delete("leaderboard");
    redisTemplate.opsForZSet().add("leaderboard", "player1", 100);
    redisTemplate.opsForZSet().add("leaderboard", "player2", 200);

    Set<String> topPlayers = redisTemplate.opsForZSet()
        .reverseRange("leaderboard", 0, -1);//Descending order(Highest score first)

    assertThat(topPlayers)
        .containsExactly("player2", "player1");
  }
}
