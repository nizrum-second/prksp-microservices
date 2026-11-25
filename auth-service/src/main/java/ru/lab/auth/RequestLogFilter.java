package ru.lab.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Логирует каждое обращение (метод, URL, IP) и обеспечивает сквозной идентификатор запроса X-Request-ID:
 * берёт его из входящего заголовка (или создаёт новый), кладёт в MDC (попадает во все строки лога)
 * и возвращает клиенту в заголовке ответа.
 */
@Component
public class RequestLogFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger("access");

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return request.getRequestURI().startsWith("/actuator"); // не засоряем логи пробами
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String forwarded = req.getHeader("X-Forwarded-For"); // KrakenD/Traefik кладут сюда IP клиента
        String ip = (forwarded != null && !forwarded.isBlank())
                ? forwarded.split(",")[0].trim() : req.getRemoteAddr();
        String url = req.getRequestURI() + (req.getQueryString() != null ? "?" + req.getQueryString() : "");

        String requestId = req.getHeader("X-Request-ID");
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }
        res.setHeader("X-Request-ID", requestId);

        MDC.put("http_method", req.getMethod());
        MDC.put("url", url);
        MDC.put("client_ip", ip);
        MDC.put("request_id", requestId);
        try {
            log.info("HTTP {} {} ip={} requestId={}", req.getMethod(), url, ip, requestId);
            chain.doFilter(req, res);
        } finally {
            MDC.remove("http_method");
            MDC.remove("url");
            MDC.remove("client_ip");
            MDC.remove("request_id");
        }
    }
}
