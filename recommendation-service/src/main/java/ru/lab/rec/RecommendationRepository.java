package ru.lab.rec;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Блокирующий JDBC-доступ. Вызывается из реактивного кода только через Schedulers.boundedElastic(). */
@Repository
public class RecommendationRepository {

    public record Candidate(UUID videoId, String url, String title, String category,
                            double averageLevel, int ratingsCount, double userLevel, double deviation) {}

    /**
     * Логика рекомендации: любимая категория, видео ещё не оценено пользователем (не просмотрено),
     * сложность отличается от уровня пользователя не более чем на rec.max-deviation.
     */
    private static final String BASE_SQL = """
            SELECT v.video_id, v.url, v.title, v.category, v.avg_level, v.ratings_count,
                   u.language_level AS user_level,
                   ABS(v.avg_level - u.language_level) AS deviation
            FROM videos_replica v
            JOIN user_profiles u ON u.user_id = ?
            WHERE v.category = u.favorite_category
              AND v.ratings_count > 0
              AND NOT EXISTS (SELECT 1 FROM watched w WHERE w.user_id = u.user_id AND w.video_id = v.video_id)
              AND ABS(v.avg_level - u.language_level) <= ?
            """;

    private static final RowMapper<Candidate> MAPPER = (rs, i) -> new Candidate(
            rs.getObject("video_id", UUID.class), rs.getString("url"), rs.getString("title"),
            rs.getString("category"), rs.getDouble("avg_level"), rs.getInt("ratings_count"),
            rs.getDouble("user_level"), rs.getDouble("deviation"));

    private final JdbcTemplate jdbc;
    private final double maxDeviation;

    public RecommendationRepository(JdbcTemplate jdbc, @Value("${rec.max-deviation}") double maxDeviation) {
        this.jdbc = jdbc;
        this.maxDeviation = maxDeviation;
    }

    public List<Candidate> findCandidates(UUID userId, int limit) {
        Integer profiles = jdbc.queryForObject("SELECT COUNT(*) FROM user_profiles WHERE user_id = ?",
                Integer.class, userId);
        if (profiles == null || profiles == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "user profile is not synchronized yet");
        }
        return jdbc.query(BASE_SQL + " ORDER BY deviation, v.ratings_count DESC LIMIT ?",
                MAPPER, userId, maxDeviation, limit);
    }

    /** Подходит ли конкретное видео пользователю прямо сейчас (для SSE-потока). */
    public Optional<Candidate> findCandidate(UUID userId, UUID videoId) {
        return jdbc.query(BASE_SQL + " AND v.video_id = ?", MAPPER, userId, maxDeviation, videoId)
                .stream().findFirst();
    }
}
