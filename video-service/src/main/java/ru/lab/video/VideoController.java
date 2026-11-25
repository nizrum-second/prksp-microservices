package ru.lab.video;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import ru.lab.video.VideoService.VideoDto;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/videos")
public class VideoController {

    public record VideoRequest(String url, String title, String category) {}

    public record RatingRequest(String level) {}

    private final VideoService service;
    private final EventPublisher events;

    public VideoController(VideoService service, EventPublisher events) {
        this.service = service;
        this.events = events;
    }

    @GetMapping
    public List<VideoDto> list(@RequestParam(required = false) String category,
                               @RequestParam(defaultValue = "0") int page,
                               @RequestParam(defaultValue = "20") int size) {
        return service.list(category, Math.max(page, 0), Math.max(1, Math.min(size, 100)));
    }

    @GetMapping("/{id}")
    public VideoDto get(@PathVariable UUID id) {
        return service.find(id).orElseThrow(this::notFound);
    }

    @PostMapping
    public ResponseEntity<VideoDto> create(@RequestHeader(value = "X-User-Id", required = false) String userHeader,
                                           @RequestBody VideoRequest r) {
        UUID user = currentUser(userHeader);
        VideoDto v = service.create(user, required(r.url(), "url"), required(r.title(), "title"),
                required(r.category(), "category").toLowerCase());
        events.video("VIDEO_CREATED", v);
        return ResponseEntity.status(HttpStatus.CREATED).body(v);
    }

    @PutMapping("/{id}")
    public VideoDto update(@RequestHeader(value = "X-User-Id", required = false) String userHeader,
                           @PathVariable UUID id, @RequestBody VideoRequest r) {
        UUID user = currentUser(userHeader);
        checkOwner(id, user);
        VideoDto v = service.update(id, required(r.url(), "url"), required(r.title(), "title"),
                required(r.category(), "category").toLowerCase());
        events.video("VIDEO_UPDATED", v);
        return v;
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@RequestHeader(value = "X-User-Id", required = false) String userHeader,
                                       @PathVariable UUID id) {
        UUID user = currentUser(userHeader);
        VideoDto v = checkOwner(id, user);
        service.delete(id);
        events.video("VIDEO_DELETED", v);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/rating")
    public ResponseEntity<VideoDto> rate(@RequestHeader(value = "X-User-Id", required = false) String userHeader,
                                         @PathVariable UUID id, @RequestBody RatingRequest r) {
        UUID user = currentUser(userHeader);
        int level = Levels.toNumber(r.level());
        get(id); // 404, если видео нет
        VideoDto v;
        try {
            v = service.rate(id, user, level);
        } catch (DuplicateKeyException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "you have already rated this video");
        }
        events.rated(v, user, r.level().trim().toUpperCase());
        return ResponseEntity.status(HttpStatus.CREATED).body(v);
    }

    // ---- вспомогательное ----

    /** Идентификатор пользователя приходит от KrakenD в заголовке X-User-Id (берётся из JWT). */
    private UUID currentUser(String header) {
        if (header == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "X-User-Id header is missing");
        }
        try {
            return UUID.fromString(header);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "bad X-User-Id header");
        }
    }

    private VideoDto checkOwner(UUID id, UUID user) {
        VideoDto v = get(id);
        if (!v.ownerId().equals(user)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "only the owner can change this video");
        }
        return v;
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, field + " is required");
        }
        return value.trim();
    }

    private ResponseStatusException notFound() {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "video not found");
    }
}
