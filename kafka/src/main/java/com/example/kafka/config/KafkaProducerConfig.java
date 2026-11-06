package com.example.kafka.config;

import com.example.kafka.messaging.NotificationEvent;
import com.example.kafka.messaging.OrderEvent;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

  @Bean
  @Primary
  public ProducerFactory<String, OrderEvent> transactionalProducerFactory(
      KafkaProperties properties,
      ObjectProvider<SslBundles> sslBundles
  ) {
    DefaultKafkaProducerFactory<String, OrderEvent> factory =
        new DefaultKafkaProducerFactory<>(
            properties.buildProducerProperties(sslBundles.getIfAvailable())
        );

    factory.setTransactionIdPrefix(properties.getProducer().getTransactionIdPrefix());

    return factory;
  }

  @Bean
  public ProducerFactory<String, OrderEvent> nonTransactionalEventProducerFactory(
      KafkaProperties kafkaProperties,
      ObjectProvider<SslBundles> sslBundles
  ) {
    var properties = kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable());
    return new DefaultKafkaProducerFactory<>(properties);
  }

  @Bean
  public ProducerFactory<String, NotificationEvent> nonTransactionalNotificationProducerFactory(
      KafkaProperties kafkaProperties,
      ObjectProvider<SslBundles> sslBundles
  ) {
    var properties = kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable());
    return new DefaultKafkaProducerFactory<>(properties);
  }

  @Bean
  public KafkaTemplate<String, NotificationEvent> nonTransactionalNotificationKafkaTemplate(
      @Qualifier("nonTransactionalNotificationProducerFactory")
      ProducerFactory<String, NotificationEvent> producerFactory
  ) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public KafkaTemplate<String, OrderEvent> nonTransactionalKafkaTemplate(
      @Qualifier("nonTransactionalEventProducerFactory")
      ProducerFactory<String, OrderEvent> producerFactory
  ) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public KafkaTemplate<String, OrderEvent> kafkaTemplate(
      @Qualifier("transactionalProducerFactory")
      ProducerFactory<String, OrderEvent> producerFactory
  ) {
    return new KafkaTemplate<>(producerFactory);
  }
}
