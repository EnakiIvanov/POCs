package com.example.aop.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
@ConditionalOnProperty(
    prefix = "aop.aspect",
    name = "after.enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class AfterAspect {

  /**
   * Executes after the matched method, regardless of whether it returns or throws.
   */
  @After("execution(* com.example.aop.service..*(..))")
  public void afterAdvice(JoinPoint joinPoint) {
    log.info("[AFTER] Finished: {}", joinPoint.getSignature().getName());
  }
}
