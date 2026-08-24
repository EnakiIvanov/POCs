package com.example.aop.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
@ConditionalOnProperty(
    prefix = "aop.aspect",
    name = "before.enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class BeforeAspect {

  /**
   * Defines a reusable pointcut for all methods in the service layer. A named pointcut is optional
   * and can be referenced by advices in the same or different aspect classes.
   */
  @Pointcut("execution(* com.example.aop.service..*(..))")
  public void serviceLayer() {
  }

  /**
   * Executes before the matched method.
   */
  @Before("serviceLayer()")
  public void beforeAdvice(JoinPoint joinPoint) {
    log.info("[BEFORE] Entering: {}", joinPoint.getSignature().getName());
  }
}
