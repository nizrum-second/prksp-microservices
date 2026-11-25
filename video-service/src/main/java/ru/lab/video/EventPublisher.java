package ru.lab.video;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Все события о видео идут в один топик video-events с ключом videoId -> порядок событий одного видео сохраняется.
 * Каждое событие содержит eventId (для идемпотентности у потребителя), а сквозной X-Request-ID передаётся
 * заголовком сообщения.
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private static final String TOPIC = "video-events";

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    public EventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
        this.kafka = kafka;
        this.mapper = mapper;
    }

    /** type: VIDEO_CREATED | VIDEO_UPDATED | VIDEO_DELETED */
    public void video(String type, VideoService.VideoDto v) {
        send(v.id(), base(type, v));
    }

    public void rated(VideoService.VideoDto v, UUID userId, String level) {
        Map<String, Object> m = base("VIDEO_RATED", v);
        m.put("userId", userId.toString());
        m.put("level", level);
        send(v.id(), m);
    }

    private Map<String, Object> base(String type, VideoService.VideoDto v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", UUID.randomUUID().toString());
        m.put("type", type);
        m.put("videoId", v.id().toString());
        m.put("url", v.url());
        m.put("title", v.title());
        m.put("category", v.category());
        m.put("avgLevel", v.averageLevel());
        m.put("ratingsCount", v.ratingsCount());
        return m;
    }

    private void send(UUID key, Map<String, Object> payload) {
        try {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, key.toString(), mapper.writeValueAsString(payload));
            String requestId = MDC.get("request_id");
            if (requestId != null) {
                record.headers().add("X-Request-ID", requestId.getBytes(StandardCharsets.UTF_8));
            }
            kafka.send(record);
            log.info("event {} published to {} (eventId={})", payload.get("type"), TOPIC, payload.get("eventId"));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
