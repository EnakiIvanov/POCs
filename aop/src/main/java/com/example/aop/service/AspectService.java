package com.example.aop.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AspectService {

  public String successfulMethod(String name) {
    log.info("💼 Executing successfulMethod()");
    return "Hello, " + name;
  }

  public void voidMethod() {
    log.info("💼 Executing voidMethod()");
  }

  public String slowMethod() throws InterruptedException {
    log.info("💼 Executing slowMethod()");
    Thread.sleep(500); // simulate work
    return "Done";
  }

  public void failingMethod() {
    log.info("💼 Executing failingMethod()");
    throw new RuntimeException("Something went wrong ❌");
  }
}
