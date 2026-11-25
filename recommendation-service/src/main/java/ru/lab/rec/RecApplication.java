package ru.lab.rec;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class RecApplication {

    public static void main(String[] args) {
        SpringApplication.run(RecApplication.class, args);
    }

    // Топики создаются идемпотентно, чтобы порядок запуска сервисов не имел значения
    @Bean
    NewTopic userRegisteredTopic() {
        return TopicBuilder.name("user-registered").partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic videoEventsTopic() {
        return TopicBuilder.name("video-events").partitions(1).replicas(1).build();
    }
}
