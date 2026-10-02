package com.example.websocket.stomp;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@Profile("stomp")
@RequiredArgsConstructor
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

  private static final long HEARTBEAT_SERVER_TO_CLIENT_MS = 10_000;
  private static final long HEARTBEAT_CLIENT_TO_SERVER_MS = 10_000;

  private final WebSocketChannelInterceptor interceptor;
  private final StompErrorHandler stompErrorHandler;

  @Override
  public void configureClientInboundChannel(ChannelRegistration registration) {
    registration.interceptors(interceptor);
  }

  @Override
  public void configureMessageBroker(MessageBrokerRegistry registry) {
    registry.enableSimpleBroker("/topic")
        .setHeartbeatValue(new long[] {HEARTBEAT_SERVER_TO_CLIENT_MS, HEARTBEAT_CLIENT_TO_SERVER_MS})
        .setTaskScheduler(taskScheduler());
    registry.setApplicationDestinationPrefixes("/app");
  }

  @Override
  public void registerStompEndpoints(StompEndpointRegistry registry) {
    registry
        .setErrorHandler(stompErrorHandler)
        .addEndpoint("/ws")
        .setAllowedOrigins("*");
  }

  @Bean
  public TaskScheduler taskScheduler() {
    var scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.setThreadNamePrefix("stomp-heartbeat-");
    return scheduler;
  }
}
