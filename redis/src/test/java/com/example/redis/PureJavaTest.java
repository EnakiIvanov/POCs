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

  /**
   * Strings are the basic building block in Redis — everything else internally is built on top of them
   */
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

  /**
   * Hashes are ideal when you want to store structured data without serializing a whole object.
   */
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
      var storedUser = jedis.hgetAll("user:1");// get all fields

      assertThat(actualEmail)
          .isEqualTo(expectedEmail);

      assertThat(storedUser)
          .isEqualTo(user);
    }
  }

  /**
   * Lists are commonly used for queues, task processing, or simple ordered logs.
   */
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

  /**
   * Sets are great when you need fast membership checks — for example, online users or unique tags
   */
  @Test
  void set_unique_unordered_collection() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("users:set"); // clean before test

      jedis.sadd("users:set", "user1");
      jedis.sadd("users:set", "user2");
      jedis.sadd("users:set", "user2"); // duplicate -> ignored
      Set<String> users = jedis.smembers("users:set"); // order not guaranteed

      assertThat(users)
          .hasSize(2)
          .containsExactlyInAnyOrder("user1", "user2");
    }
  }

  /**
   * Sorted sets are perfect for leaderboards or ranking system
   */
  @Test
  void sorted_set_ordered_by_score() {
    try (Jedis jedis = new Jedis(redisHost, redisPort)) {
      jedis.del("leaderboard:zset"); // clean before test

      jedis.zadd("leaderboard:zset", 100, "player1");
      jedis.zadd("leaderboard:zset", 200, "player2");
      jedis.zadd("leaderboard:zset", 200, "player2");

      List<String> highestScoreFirst = jedis.zrevrange("leaderboard:zset", 0, -1);
      // zrevrange -> highest score first
      // zrange -> lowest score fist

      assertThat(highestScoreFirst)
          .hasSize(2)
          .containsExactly("player2", "player1"); // highest to lowest score
    }
  }
}
