package com.banco.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {
    public static final String TRANSACCIONES = "banco-transacciones";
    public static final String ALERTAS = "banco-alertas";

    @Bean
    NewTopic transaccionesTopic() {
        return TopicBuilder.name(TRANSACCIONES).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic alertasTopic() {
        return TopicBuilder.name(ALERTAS).partitions(3).replicas(1).build();
    }
}
