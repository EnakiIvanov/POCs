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
class BeforeAspectTest {

  @Nested
  @SpringBootTest(properties = "aop.aspect.before.enabled=true")
  class Enabled {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AspectService service;

    @Test
    void aspectIsLoaded() {
      assertThat(context.getBeansOfType(BeforeAspect.class))
          .isNotEmpty();
    }

    @Test
    void successfulMethod_returnsResult(CapturedOutput output) {
      service.voidMethod();

      assertThat(output)
          .containsSubsequence("[BEFORE] Entering: voidMethod", "Executing voidMethod()");
    }
  }

  @Nested
  @SpringBootTest(properties = "aop.aspect.before.enabled=false")
  class Disabled {

    @Autowired
    private ApplicationContext context;

    @Test
    void aspectIsNotLoaded() {
      assertThat(context.getBeansOfType(BeforeAspect.class))
          .isEmpty();
    }
  }
}
