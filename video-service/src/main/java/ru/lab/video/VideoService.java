package ru.lab.video;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class VideoService {

    /** averageLevel — среднее (1..6), currentLevel — оно же в виде A1..C2. Оба null, пока нет оценок. */
    public record VideoDto(UUID id, String url, String title, String category, UUID ownerId,
                           int ratingsCount, Double averageLevel, String currentLevel) {}

    private static final RowMapper<VideoDto> MAPPER = (rs, i) -> {
        int count = rs.getInt("ratings_count");
        int sum = rs.getInt("rating_sum");
        Double avg = count == 0 ? null : (double) sum / count;
        return new VideoDto(
                rs.getObject("id", UUID.class), rs.getString("url"), rs.getString("title"),
                rs.getString("category"), rs.getObject("owner_id", UUID.class),
                count, avg, avg == null ? null : Levels.toName(avg));
    };

    private final JdbcTemplate jdbc;

    public VideoService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<VideoDto> list(String category, int page, int size) {
        List<Object> args = new ArrayList<>();
        String sql = "SELECT * FROM videos";
        if (category != null && !category.isBlank()) {
            sql += " WHERE category = ?";
            args.add(category.trim().toLowerCase());
        }
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?";
        args.add(size);
        args.add(page * size);
        return jdbc.query(sql, MAPPER, args.toArray());
    }

    public Optional<VideoDto> find(UUID id) {
        return jdbc.query("SELECT * FROM videos WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public VideoDto create(UUID owner, String url, String title, String category) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO videos(id, url, title, category, owner_id) VALUES (?,?,?,?,?)",
                id, url, title, category, owner);
        return find(id).orElseThrow();
    }

    public VideoDto update(UUID id, String url, String title, String category) {
        jdbc.update("UPDATE videos SET url = ?, title = ?, category = ? WHERE id = ?", url, title, category, id);
        return find(id).orElseThrow();
    }

    public void delete(UUID id) {
        jdbc.update("DELETE FROM videos WHERE id = ?", id); // оценки удаляются каскадно
    }

    /**
     * Оценка сложности. Одна транзакция: запись оценки + пересчёт среднего.
     * Повторная оценка тем же пользователем нарушает PRIMARY KEY -> DuplicateKeyException.
     */
    @Transactional
    public VideoDto rate(UUID videoId, UUID userId, int level) {
        jdbc.update("INSERT INTO video_ratings(video_id, user_id, level) VALUES (?,?,?)", videoId, userId, level);
        jdbc.update("UPDATE videos SET ratings_count = ratings_count + 1, rating_sum = rating_sum + ? WHERE id = ?",
                level, videoId);
        return find(videoId).orElseThrow();
    }
}
