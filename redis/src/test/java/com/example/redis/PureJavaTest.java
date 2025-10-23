package com.example.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.redis.config.RedisConfig;
import com.redis.testcontainers.RedisContainer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import redis.clients.jedis.Jedis;

@SpringBootTest
@Import(RedisConfig.class)
class PureJavaTest {

  @Autowired
  private RedisContainer redisContainer;
  private String redisHost;
  private int redisPort;

  @BeforeEach
  void setup() {
    redisHost = redisContainer.getHost();
    redisPort = redisContainer.getFirstMappedPort();
  }

  @Test
  void string_simple_key_value() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("productId"); // clean before test

      String expectedValue = "1";
      jedis.set("productId", expectedValue);   // store key-value
      String actualValue = jedis.get("productId"); // retrieve by key

      assertThat(actualValue)
          .isEqualTo(expectedValue);
    }
  }

  @Test
  void hash_objects_with_fields() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("user:1"); // clean before test

      String expectedEmail = "alice@example.com";
      Map<String, String> user = new HashMap<>();
      user.put("name", "Alice");
      user.put("email", expectedEmail);

      jedis.hset("user:1", user); // store as hash
      String actualEmail = jedis.hget("user:1", "email"); // get a single field

      assertThat(actualEmail)
          .isEqualTo(expectedEmail);
    }
  }

  @Test
  void list_ordered_collection() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("tasks:list"); // clean before test

      jedis.lpush("tasks:list", "task1"); // pushes to the left, which is the beginning of the list
      jedis.rpush("tasks:list", "task2"); // pushes to the right, which is the end of the list
      List<String> tasks = jedis.lrange("tasks:list", 0, -1); // get all elements

      assertThat(tasks)
          .containsExactly("task1", "task2"); // preserves insertion order
    }
  }

  @Test
  void set_unique_unordered_collection() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("users:set"); // clean before test

      jedis.sadd("users:set", "user1");
      jedis.sadd("users:set", "user2");
      jedis.sadd("users:set", "user2"); // duplicate → ignored
      Set<String> users = jedis.smembers("users:set"); // order not guaranteed

      assertThat(users)
          .hasSize(2)
          .containsExactlyInAnyOrder("user1", "user2");
    }
  }

  @Test
  void sorted_set_ordered_by_score() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("leaderboard:zset"); // clean before test

      jedis.zadd("leaderboard:zset", 100, "player1");
      jedis.zadd("leaderboard:zset", 200, "player2");
      jedis.zadd("leaderboard:zset", 200, "player2");

      List<String> topPlayers = jedis.zrevrange("leaderboard:zset", 0, -1);
      // zrevrange -> highest score first

      assertThat(topPlayers)
          .hasSize(2)
          .containsExactly("player2", "player1"); // descending order
    }
  }
}
