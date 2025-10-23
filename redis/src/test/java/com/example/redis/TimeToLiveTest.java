package com.example.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.example.redis.config.RedisConfig;
import com.example.redis.model.User;
import com.example.redis.repository.UserRepository;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

@DataRedisTest
@Import(RedisConfig.class)
class TimeToLiveTest {

  @Autowired
  private StringRedisTemplate redisTemplate;

  @Autowired
  private UserRepository userRepository;

  @Test
  void refresh_time_to_live() {
    User user = User.builder()
        .id("1")
        .age(20)
        .name("John")
        .updated(LocalDateTime.now())
        .build();
    userRepository.save(user);

    await().atMost(10, TimeUnit.SECONDS)
        .until(() -> userRepository.findById("1").isPresent());

    await().atMost(15, TimeUnit.SECONDS)
        .until(() -> userRepository.findById("1").isEmpty());
  }

  @Test
  void shouldDemonstrateTtlAndRenewalOnEntity() {
    String id = "1";
    User user = User.builder()
        .id(id)
        .age(20)
        .name("John")
        .updated(LocalDateTime.now())
        .build();
    userRepository.save(user);

    // Check initial TTL in Redis
    Long ttl1 = redisTemplate.getExpire("users:" + id, TimeUnit.SECONDS);
    System.out.println("initial: " + ttl1);
    assertThat(ttl1).isGreaterThan(0);

    // Wait until TTL is close to expiring
    await().atMost(4, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          Long ttl = redisTemplate.getExpire("users:" + id, TimeUnit.SECONDS);
          System.out.println("about to expire: " + ttl);
          assertThat(ttl).isLessThanOrEqualTo(1);
        });

    // Renew TTL by saving again with longer TTL
    User renewed = User.builder()
        .id(id)
        .age(20)
        .name("John")
        .updated(LocalDateTime.now())
        .build();
    userRepository.save(renewed);

    // TTL should be refreshed
    Long ttl2 = redisTemplate.getExpire("users:" + id, TimeUnit.SECONDS);
    System.out.println("renewed: " + ttl2);
    assertThat(ttl2).isGreaterThan(2);

    // Ensure entity is still retrievable
    Optional<User> reloaded = userRepository.findById(id);
    assertThat(reloaded).isPresent();
  }

  @Test
  void shouldDemonstrateTtlAndRenewal() {
    String key = "demo:ttl";
    String value = "hello";

    // set TTL to 2 seconds
    redisTemplate.opsForValue().set(key, value, 2, TimeUnit.SECONDS);

    // initial TTL should be close to 2
    Long ttl1 = redisTemplate.getExpire(key, TimeUnit.SECONDS);
    System.out.println("initial: " + ttl1);
    assertThat(ttl1).isGreaterThan(0);

    // Wait until TTL is about to expire (but not expired yet)
    await().atMost(2, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          Long ttl = redisTemplate.getExpire(key, TimeUnit.SECONDS);
          System.out.println("about to expire: " + ttl);
          assertThat(ttl).isLessThanOrEqualTo(1);
        });

    // "Renew" by updating value with new TTL
    redisTemplate.opsForValue().set(key, value, 5, TimeUnit.SECONDS);

    // assert TTL has been renewed
    Long ttl2 = redisTemplate.getExpire(key, TimeUnit.SECONDS);
    System.out.println("renewed: " + ttl2);
    assertThat(ttl2).isGreaterThan(2);
  }
}
