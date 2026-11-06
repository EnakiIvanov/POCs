package com.example.kafka.config;

import com.example.kafka.messaging.OrderEvent;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

  @Bean
  public DefaultErrorHandler errorHandler(
      @Qualifier("kafkaTemplate")
      KafkaTemplate<String, OrderEvent> kafkaTemplate) {
    var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
        (cRecord, exception) ->
            new TopicPartition(cRecord.topic() + ".DLT", cRecord.partition()));

    FixedBackOff backOff = new FixedBackOff(5_000L, 3L);

    return new DefaultErrorHandler(recoverer, backOff);
  }
}
