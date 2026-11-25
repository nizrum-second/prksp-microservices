package ru.lab.rec;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Comparator;
import java.util.UUID;

@RestController
public class RecommendationController {

    private final RecommendationRepository repository;
    private final VideoClient videoClient;
    private final VideoChangeBus bus;

    public RecommendationController(RecommendationRepository repository, VideoClient videoClient, VideoChangeBus bus) {
        this.repository = repository;
        this.videoClient = videoClient;
        this.bus = bus;
    }

    /**
     * Рекомендации: кандидаты берутся из локальной БД (блокирующий JDBC выносится на boundedElastic),
     * затем параллельно (до 4 одновременно) подтверждаются запросами к video-service через WebClient.
     */
    @GetMapping("/api/recommendations")
    public Flux<Recommendation> recommend(@RequestHeader(value = "X-User-Id", required = false) String userHeader,
                                          @RequestParam(defaultValue = "1") int limit) {
        UUID userId = parseUser(userHeader);
        int n = Math.max(1, Math.min(limit, 50));
        return Mono.fromCallable(() -> repository.findCandidates(userId, n))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(Flux::fromIterable)
                .flatMap(videoClient::enrich, 4)
                .sort(Comparator.comparingDouble(Recommendation::deviation));
    }

    /**
     * Живой поток рекомендаций (Server-Sent Events). Подписчик получает событие "recommendation" каждый раз,
     * когда после изменения какого-либо видео (новое видео, новая оценка сложности) оно начинает подходить
     * ему по категории, уровню и ещё не оценено им. Раз в 15 секунд отправляется keep-alive.
     */
    @GetMapping(path = "/api/recommendations/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> stream(@RequestHeader(value = "X-User-Id", required = false) String userHeader) {
        UUID userId = parseUser(userHeader);

        Flux<ServerSentEvent<Object>> updates = bus.changes()
                .flatMap(videoId -> Mono.fromCallable(() -> repository.findCandidate(userId, videoId))
                        .subscribeOn(Schedulers.boundedElastic())
                        .flatMap(opt -> opt.map(Mono::just).orElseGet(Mono::empty)))
                .map(c -> ServerSentEvent.<Object>builder(c)
                        .event("recommendation")
                        .id(c.videoId().toString())
                        .build());

        Flux<ServerSentEvent<Object>> keepAlive = Flux.interval(Duration.ofSeconds(15))
                .map(i -> ServerSentEvent.<Object>builder().comment("keep-alive").build());

        Mono<ServerSentEvent<Object>> hello = Mono.just(
                ServerSentEvent.<Object>builder("subscribed").event("CONNECTED").build());

        return Flux.concat(hello, Flux.merge(updates, keepAlive));
    }

    private static UUID parseUser(String header) {
        try {
            return UUID.fromString(header);
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "X-User-Id header is missing or bad");
        }
    }
}
