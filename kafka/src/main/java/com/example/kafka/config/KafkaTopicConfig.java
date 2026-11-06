package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

  //Produce and Consume String
  @Bean
  public NewTopic messageTopic() {
    return TopicBuilder.name("poc.messages")
        .partitions(3)
        .replicas(1)
        .build();
  }

  //Produce and Consume JSON
  @Bean
  public NewTopic orderEventsTopic() {
    return TopicBuilder.name("poc.order-events")
        .partitions(3)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic notificationsTopic() {
    return TopicBuilder.name("poc.notifications")
        .partitions(3)
        .replicas(1)
        .build();
  }

  //Dead Letter Topic
  @Bean
  public NewTopic orderEventsDltTopic() {
    return TopicBuilder.name("poc.order-events.DLT")
        .partitions(3)
        .replicas(1)
        .build();
  }
}
