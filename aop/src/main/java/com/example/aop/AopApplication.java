package com.example.aop;

import com.example.aop.service.AspectService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@Slf4j
@SpringBootApplication
public class AopApplication {

  public static void main(String[] args) {
    SpringApplication.run(AopApplication.class, args);
  }

  @Bean
  @Profile("!test")
  ApplicationRunner applicationRunner(AspectService service) {
    return args -> {
      log.info("\n--- SUCCESSFUL ---");
      service.successfulMethod("Spring");

      log.info("\n--- VOID ---");
      service.voidMethod();

      log.info("\n--- SLOW ---");
      service.slowMethod();

      log.info("\n--- FAILING ---");
      try {
        service.failingMethod();
      } catch (Exception ignored) {
      }
    };
  }
}
