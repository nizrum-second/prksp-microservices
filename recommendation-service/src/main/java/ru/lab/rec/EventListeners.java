package ru.lab.rec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Обновляет локальные копии данных по событиям из Kafka.
 *
 * Идемпотентность: у каждого события есть eventId. В одной транзакции с обработкой событие записывается
 * в processed_events (PRIMARY KEY). Повторная доставка того же события даёт 0 вставленных строк — оно пропускается.
 * Если обработка падает, откатывается и запись о событии, поэтому повторная попытка отработает полностью.
 */
@Component
public class EventListeners {

    private static final Logger log = LoggerFactory.getLogger(EventListeners.class);
    private static final String[] LEVELS = {"A1", "A2", "B1", "B2", "C1", "C2"};

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;
    private final VideoChangeBus bus;

    public EventListeners(JdbcTemplate jdbc, ObjectMapper mapper, TransactionTemplate tx, VideoChangeBus bus) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.tx = tx;
        this.bus = bus;
    }

    @KafkaListener(topics = "user-registered", groupId = "recommendation")
    public void onUserRegistered(ConsumerRecord<String, String> record) throws Exception {
        JsonNode n = mapper.readTree(record.value());
        withRequestId(record, () -> {
            boolean processed = processOnce(n, event -> jdbc.update("""
                    INSERT INTO user_profiles(user_id, favorite_category, language_level) VALUES (?,?,?)
                    ON CONFLICT (user_id) DO UPDATE
                      SET favorite_category = EXCLUDED.favorite_category, language_level = EXCLUDED.language_level
                    """,
                    UUID.fromString(event.get("userId").asText()),
                    event.get("favoriteCategory").asText(),
                    levelToNumber(event.get("languageLevel").asText())));
            if (processed) {
                log.info("user profile saved: {}", n.get("userId").asText());
            }
        });
    }

    @KafkaListener(topics = "video-events", groupId = "recommendation")
    public void onVideoEvent(ConsumerRecord<String, String> record) throws Exception {
        JsonNode n = mapper.readTree(record.value());
        String type = n.get("type").asText();
        UUID videoId = UUID.fromString(n.get("videoId").asText());

        withRequestId(record, () -> {
            boolean processed = processOnce(n, event -> applyVideoEvent(type, videoId, event));
            if (processed) {
                log.info("video event {} processed (video {})", type, videoId);
                if (!"VIDEO_DELETED".equals(type)) {
                    bus.publish(videoId);   // уведомляем SSE-подписчиков
                }
            }
        });
    }

    private void applyVideoEvent(String type, UUID videoId, JsonNode n) {
        double avg = n.path("avgLevel").asDouble(0);   // null -> 0 (оценок ещё нет)
        int count = n.path("ratingsCount").asInt(0);
        switch (type) {
            case "VIDEO_CREATED", "VIDEO_UPDATED" -> jdbc.update("""
                    INSERT INTO videos_replica(video_id, url, title, category, avg_level, ratings_count)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT (video_id) DO UPDATE
                      SET url = EXCLUDED.url, title = EXCLUDED.title, category = EXCLUDED.category,
                          avg_level = EXCLUDED.avg_level, ratings_count = EXCLUDED.ratings_count
                    """,
                    videoId, n.get("url").asText(), n.get("title").asText(), n.get("category").asText(), avg, count);
            case "VIDEO_RATED" -> {
                // пользователь оценил видео -> оно считается просмотренным
                jdbc.update("INSERT INTO watched(user_id, video_id) VALUES (?,?) ON CONFLICT DO NOTHING",
                        UUID.fromString(n.get("userId").asText()), videoId);
                jdbc.update("UPDATE videos_replica SET avg_level = ?, ratings_count = ? WHERE video_id = ?",
                        avg, count, videoId);
            }
            case "VIDEO_DELETED" -> {
                jdbc.update("DELETE FROM videos_replica WHERE video_id = ?", videoId);
                jdbc.update("DELETE FROM watched WHERE video_id = ?", videoId);
            }
            default -> log.warn("unknown event type {}", type);
        }
    }

    /** @return true, если событие обработано; false, если это дубликат. */
    private boolean processOnce(JsonNode event, Consumer<JsonNode> action) {
        String eventId = event.path("eventId").asText(null);
        boolean[] processed = {false};
        tx.executeWithoutResult(status -> {
            if (eventId != null) {
                int inserted = jdbc.update(
                        "INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT DO NOTHING",
                        UUID.fromString(eventId));
                if (inserted == 0) {
                    log.info("duplicate event {} ignored", eventId);
                    return;
                }
            }
            action.accept(event);
            processed[0] = true;
        });
        return processed[0];
    }

    /** Кладёт X-Request-ID из заголовка Kafka-сообщения в MDC, чтобы он попал в логи. */
    private void withRequestId(ConsumerRecord<String, String> record, Runnable body) {
        Header header = record.headers().lastHeader("X-Request-ID");
        if (header != null) {
            MDC.put("request_id", new String(header.value(), StandardCharsets.UTF_8));
        }
        try {
            body.run();
        } finally {
            MDC.remove("request_id");
        }
    }

    private static int levelToNumber(String level) {
        for (int i = 0; i < LEVELS.length; i++) {
            if (LEVELS[i].equalsIgnoreCase(level)) {
                return i + 1;
            }
        }
        throw new IllegalArgumentException("unknown level " + level);
    }
}
