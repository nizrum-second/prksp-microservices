package ru.lab.rec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Реактивный аналог RequestLogFilter: логирует метод, URL, IP и сквозной X-Request-ID,
 * возвращает идентификатор в ответе и кладёт его в контекст Reactor (оттуда его берёт WebClient).
 */
@Component
public class RequestIdFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger("access");

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getURI().getRawPath();
        if (path.startsWith("/actuator")) {
            return chain.filter(exchange);
        }
        String requestId = exchange.getRequest().getHeaders().getFirst("X-Request-ID");
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }
        exchange.getResponse().getHeaders().set("X-Request-ID", requestId);

        String forwarded = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
        String ip = (forwarded != null && !forwarded.isBlank()) ? forwarded.split(",")[0].trim()
                : (remote != null && remote.getAddress() != null ? remote.getAddress().getHostAddress() : "-");
        String query = exchange.getRequest().getURI().getRawQuery();
        String url = path + (query != null ? "?" + query : "");
        String method = exchange.getRequest().getMethod().name();

        MDC.put("http_method", method);
        MDC.put("url", url);
        MDC.put("client_ip", ip);
        MDC.put("request_id", requestId);
        try {
            log.info("HTTP {} {} ip={} requestId={}", method, url, ip, requestId);
        } finally {
            MDC.remove("http_method");
            MDC.remove("url");
            MDC.remove("client_ip");
            MDC.remove("request_id");
        }
        String rid = requestId;
        return chain.filter(exchange).contextWrite(ctx -> ctx.put("requestId", rid));
    }
}
