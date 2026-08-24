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
class AfterAspectTest {

  @Nested
  @SpringBootTest(properties = "aop.aspect.after.enabled=true")
  class Enabled {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AspectService service;

    @Test
    void aspectIsLoaded() {
      assertThat(context.getBeansOfType(AfterAspect.class))
          .isNotEmpty();
    }

    @Test
    void voidMethod_printsLog(CapturedOutput output) {
      service.voidMethod();

      assertThat(output)
          .containsSubsequence("Executing voidMethod()", "[AFTER] Finished: voidMethod");
    }
  }

  @Nested
  @SpringBootTest(properties = "aop.aspect.after.enabled=false")
  class Disabled {

    @Autowired
    private ApplicationContext context;

    @Test
    void aspectIsNotLoaded() {
      assertThat(context.getBeansOfType(AfterAspect.class))
          .isEmpty();
    }
  }
}
