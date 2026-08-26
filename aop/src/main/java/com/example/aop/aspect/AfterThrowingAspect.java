package com.example.aop.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
@ConditionalOnProperty(
    prefix = "aop.aspect",
    name = "after-throwing.enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class AfterThrowingAspect {

  /**
   * Executes only when the matched method throws an exception.
   */
  @AfterThrowing(pointcut = "execution(* com.example.aop.service..*(..))\"", throwing = "ex")
  public void afterThrowingAdvice(JoinPoint joinPoint, Throwable ex) {
    log.info("[AFTER_THROWING] Failed: {} with exception: {}",
        joinPoint.getSignature().getName(), ex.getMessage());
  }
}
