package ru.lab.rec;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.UUID;

/** Шина изменений видео внутри сервиса: Kafka-потребитель публикует, SSE-подписчики получают (hot Flux). */
@Component
public class VideoChangeBus {

    private final Sinks.Many<UUID> sink = Sinks.many().multicast().directBestEffort();

    public void publish(UUID videoId) {
        sink.tryEmitNext(videoId);
    }

    public Flux<UUID> changes() {
        return sink.asFlux();
    }
}
