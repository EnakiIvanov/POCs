package com.example.aop.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
@ConditionalOnProperty(
    prefix = "aop.aspect",
    name = "after-returning.enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class AfterReturningAspect {

  /**
   * Executes only when the matched method returns successfully and provides access to the return
   * value.
   */
  @AfterReturning(pointcut = "execution(* com.example.aop.service..*(..))", returning = "result")
  public void afterReturningAdvice(JoinPoint joinPoint, Object result) {
    log.info("[AFTER_RETURNING] Completed: {} with result: {}",
        joinPoint.getSignature().getName(), result);
  }
}
