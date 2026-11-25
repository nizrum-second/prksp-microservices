package ru.lab.rec;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.util.context.ContextView;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Синхронное (с точки зрения бизнес-логики) взаимодействие с video-service через WebClient.
 * Перед выдачей рекомендации сервис запрашивает у video-service актуальные данные видео
 * (локальная копия обновляется асинхронно через Kafka и может отставать, а видео могло быть удалено).
 *
 * Защита от отказов: timeout (connect 1 c, ответ 2 c), одна повторная попытка и Circuit Breaker.
 * Если video-service недоступен — fallback на локальную копию (деградированный режим), запрос не зависает.
 */
@Component
public class VideoClient {

    private static final Logger log = LoggerFactory.getLogger(VideoClient.class);

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record VideoDto(UUID id, String url, String title, String category,
                           int ratingsCount, Double averageLevel) {}

    private final WebClient webClient;
    private final CircuitBreaker circuitBreaker;

    public VideoClient(@Value("${video-service.url}") String baseUrl, WebClient.Builder builder) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                .responseTimeout(Duration.ofSeconds(2));
        this.webClient = builder
                .baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .filter(requestIdFilter())
                .build();

        this.circuitBreaker = CircuitBreaker.of("video-service", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(6)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build());
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(e -> log.warn("circuit breaker 'video-service': {}", e.getStateTransition()));
    }

    /** Подтверждает кандидата данными из video-service; пусто, если видео уже удалено. */
    public Mono<Recommendation> enrich(RecommendationRepository.Candidate c) {
        return fetch(c.videoId())
                .map(opt -> opt.map(v -> fromVideo(c, v)))
                .onErrorResume(e -> Mono.deferContextual(ctx -> {
                    logWithRequestId(ctx, "video-service unavailable ({}), fallback to local replica for video {}",
                            e.getClass().getSimpleName(), c.videoId());
                    return Mono.just(Optional.of(fromReplica(c)));
                }))
                .flatMap(opt -> opt.map(Mono::just).orElseGet(Mono::empty));
    }

    private Mono<Optional<VideoDto>> fetch(UUID videoId) {
        return Mono.deferContextual(ctx -> {
            logWithRequestId(ctx, "GET video-service /api/videos/{}", videoId);
            return webClient.get().uri("/api/videos/{id}", videoId)
                    .<Optional<VideoDto>>exchangeToMono(resp -> {
                        if (resp.statusCode().value() == 404) {
                            return Mono.just(Optional.<VideoDto>empty());   // видео удалено — это не сбой сервиса
                        }
                        if (resp.statusCode().is2xxSuccessful()) {
                            return resp.bodyToMono(VideoDto.class).map(Optional::of);
                        }
                        return resp.<Optional<VideoDto>>createError();
                    })
                    .timeout(Duration.ofSeconds(2))
                    .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                    .retryWhen(Retry.fixedDelay(1, Duration.ofMillis(300))
                            .filter(t -> !(t instanceof CallNotPermittedException)));
        });
    }

    private static Recommendation fromVideo(RecommendationRepository.Candidate c, VideoDto v) {
        double avg = v.averageLevel() != null ? v.averageLevel() : c.averageLevel();
        return new Recommendation(c.videoId(), v.url(), v.title(), v.category(), avg, v.ratingsCount(),
                round(Math.abs(avg - c.userLevel())), "video-service");
    }

    private static Recommendation fromReplica(RecommendationRepository.Candidate c) {
        return new Recommendation(c.videoId(), c.url(), c.title(), c.category(), c.averageLevel(),
                c.ratingsCount(), round(c.deviation()), "replica");
    }

    private static double round(double x) {
        return Math.round(x * 100.0) / 100.0;
    }

    /** Прокидывает X-Request-ID из контекста Reactor в исходящий HTTP-запрос. */
    private static ExchangeFilterFunction requestIdFilter() {
        return (request, next) -> Mono.deferContextual(ctx -> {
            String requestId = ctx.getOrDefault("requestId", "");
            ClientRequest out = requestId.isEmpty() ? request
                    : ClientRequest.from(request).header("X-Request-ID", requestId).build();
            return next.exchange(out);
        });
    }

    static void logWithRequestId(ContextView ctx, String message, Object... args) {
        String requestId = ctx.getOrDefault("requestId", "");
        MDC.put("request_id", requestId);
        try {
            log.info(message, args);
        } finally {
            MDC.remove("request_id");
        }
    }
}
