package com.example.kafka.config;

import com.example.kafka.messaging.OrderEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Configuration
public class KafkaListenerConfig {

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, OrderEvent>
  kafkaListenerContainerFactory(ConsumerFactory<String, OrderEvent> consumerFactory,
      DefaultErrorHandler errorHandler) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();

    factory.getContainerProperties()
        .setAckMode(ContainerProperties.AckMode.BATCH);
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(errorHandler);

    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, OrderEvent>
  retryTopicKafkaListenerContainerFactory(ConsumerFactory<String, OrderEvent> consumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();

    factory.getContainerProperties()
        .setAckMode(ContainerProperties.AckMode.BATCH);
    factory.setConsumerFactory(consumerFactory);

    return factory;
  }
}
