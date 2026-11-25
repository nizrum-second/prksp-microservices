package ru.lab.rec;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Компонент "kafka" в /actuator/health. Намеренно не входит в readiness-группу:
 * недоступность брокера не должна выводить сервис из балансировки (REST продолжает работать).
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    public Health health() {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2000);
        try (AdminClient client = AdminClient.create(config)) {
            int brokers = client.describeCluster().nodes().get(3, TimeUnit.SECONDS).size();
            return Health.up().withDetail("brokers", brokers).build();
        } catch (Exception e) {
            return Health.down().withDetail("error", e.getClass().getSimpleName()).build();
        }
    }
}
