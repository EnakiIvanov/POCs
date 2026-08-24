package com.example.aop.aspect;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.aop.service.AspectService;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@ExtendWith(OutputCaptureExtension.class)
class AroundAspectTest {

  @Nested
  @SpringBootTest(properties = "aop.aspect.around.enabled=true")
  class Enabled {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AspectService service;

    @Test
    void aspectIsLoaded() {
      assertThat(context.getBeansOfType(AroundAspect.class))
          .isNotEmpty();
    }

    @Test
    void slowMethod_returnsResult(CapturedOutput output) throws InterruptedException {
      String result = service.slowMethod();
      assertThat(result).isEqualTo("Done");

      assertThat(output)
          .containsSubsequence(
              "[AROUND] Before: slowMethod",
              "Executing slowMethod()",
              "[AROUND] After: slowMethod");
    }
  }

  @Nested
  @SpringBootTest(properties = "aop.aspect.around.enabled=false")
  class Disabled {

    @Autowired
    private ApplicationContext context;

    @Test
    void aspectIsNotLoaded() {
      assertThat(context.getBeansOfType(AroundAspect.class))
          .isEmpty();
    }
  }
}
