package com.example.aop.aspect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

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
class AfterThrowingAspectTest {

  @Nested
  @SpringBootTest(properties = "aop.aspect.after-throwing.enabled=true")
  class Enabled {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AspectService service;

    @Test
    void aspectIsLoaded() {
      assertThat(context.getBeansOfType(AfterThrowingAspect.class))
          .isNotEmpty();
    }

    @Test
    void failingMethod_throwsException(CapturedOutput output) {
      assertThatThrownBy(() -> service.failingMethod())
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Something went wrong");

      assertThat(output)
          .containsSubsequence(
              "Executing failingMethod()",
              "[AFTER_THROWING] Failed: failingMethod with exception: Something went wrong"
          );
    }
  }

  @Nested
  @SpringBootTest(properties = "aop.aspect.after-throwing.enabled=false")
  class Disabled {

    @Autowired
    private ApplicationContext context;

    @Test
    void aspectIsNotLoaded() {
      assertThat(context.getBeansOfType(AfterThrowingAspect.class))
          .isEmpty();
    }
  }
}
