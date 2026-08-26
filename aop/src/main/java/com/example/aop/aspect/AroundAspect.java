package com.example.aop.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
@ConditionalOnProperty(
    prefix = "aop.aspect",
    name = "around.enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class AroundAspect {

  /**
   * Wraps the matched method and controls when/how the target method is executed.
   */
  @Around("execution(* com.example.aop.service..*(..))")
  public Object aroundAdvice(ProceedingJoinPoint pjp) throws Throwable {
    String methodName = pjp.getSignature().getName();
    log.info("[AROUND] Before: {}", methodName);

    long start = System.currentTimeMillis();
    Object result = pjp.proceed();
    long duration = System.currentTimeMillis() - start;

    log.info("[AROUND] After: {} (took {} ms)", methodName, duration);
    return result;
  }
}
