package ru.lab.auth;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private static final Set<String> LEVELS = Set.of("A1", "A2", "B1", "B2", "C1", "C2");

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final JwtService jwt;
    private final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

    public AuthController(JdbcTemplate jdbc, StringRedisTemplate redis, KafkaTemplate<String, String> kafka,
                          ObjectMapper mapper, JwtService jwt) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.kafka = kafka;
        this.mapper = mapper;
        this.jwt = jwt;
    }

    public record RegisterRequest(String username, String password, String favoriteCategory, String languageLevel) {}

    public record LoginRequest(String username, String password) {}

    @PostMapping("/register")
    public ResponseEntity<Map<String, Object>> register(@RequestBody RegisterRequest r) {
        if (blank(r.username()) || blank(r.password()) || blank(r.favoriteCategory()) || blank(r.languageLevel())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "username, password, favoriteCategory, languageLevel are required");
        }
        String level = r.languageLevel().trim().toUpperCase();
        if (!LEVELS.contains(level)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "languageLevel must be one of A1,A2,B1,B2,C1,C2");
        }
        String category = r.favoriteCategory().trim().toLowerCase();
        UUID id = UUID.randomUUID();
        try {
            jdbc.update("INSERT INTO users(id, username, password_hash, favorite_category, language_level) VALUES (?,?,?,?,?)",
                    id, r.username().trim(), encoder.encode(r.password()), category, level);
        } catch (DuplicateKeyException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "username already exists");
        }
        // Событие для recommendation-service: он хранит у себя копию профиля пользователя
        // eventId нужен потребителю для защиты от повторной обработки (идемпотентность)
        publish("user-registered", id.toString(), Map.of(
                "eventId", UUID.randomUUID().toString(),
                "type", "USER_REGISTERED",
                "userId", id.toString(),
                "favoriteCategory", category,
                "languageLevel", level));
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("id", id, "username", r.username().trim()));
    }

    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody LoginRequest r) {
        String username = r.username() == null ? "" : r.username().trim();
        String password = r.password() == null ? "" : r.password();
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, password_hash FROM users WHERE username = ?", username);
        if (rows.isEmpty() || !encoder.matches(password, (String) rows.get(0).get("password_hash"))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "bad credentials");
        }
        String userId = rows.get(0).get("id").toString();
        String jti = UUID.randomUUID().toString();
        String token = jwt.create(userId, jti);
        // Токен хранится в Redis с TTL; пока ключа нет — токен считается недействительным
        redis.opsForValue().set("token:" + jti, userId, Duration.ofMinutes(jwt.ttlMinutes()));
        return Map.of("token", token, "tokenType", "Bearer", "expiresInMinutes", jwt.ttlMinutes());
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(@RequestHeader(value = "Authorization", required = false) String auth) {
        DecodedJWT token = checkToken(auth);
        redis.delete("token:" + token.getId());
        return ResponseEntity.noContent().build();
    }

    /** Заголовок X-User-Id в ответе использует Traefik (forwardAuth) для SSE-маршрута. */
    @GetMapping("/me")
    public ResponseEntity<Map<String, Object>> me(@RequestHeader(value = "Authorization", required = false) String auth) {
        DecodedJWT token = checkToken(auth);
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT id, username, favorite_category, language_level FROM users WHERE id = ?",
                UUID.fromString(token.getSubject()));
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "user not found");
        }
        return ResponseEntity.ok().header("X-User-Id", token.getSubject()).body(rows.get(0));
    }

    /** Проверяет подпись/срок токена и его наличие в Redis. */
    private DecodedJWT checkToken(String auth) {
        DecodedJWT token;
        try {
            token = jwt.verify(auth);
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "invalid token");
        }
        if (!Boolean.TRUE.equals(redis.hasKey("token:" + token.getId()))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "token revoked or expired");
        }
        return token;
    }

    /** Отправка события в Kafka; сквозной X-Request-ID передаётся заголовком сообщения. */
    private void publish(String topic, String key, Map<String, String> payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json(payload));
        String requestId = MDC.get("request_id");
        if (requestId != null) {
            record.headers().add("X-Request-ID", requestId.getBytes(StandardCharsets.UTF_8));
        }
        kafka.send(record);
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
