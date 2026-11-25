package ru.lab.rec;

import java.util.UUID;

/**
 * Элемент выдачи. source = "video-service" (данные подтверждены синхронным запросом)
 * или "replica" (video-service недоступен, использована локальная копия — деградированный режим).
 */
public record Recommendation(UUID videoId, String url, String title, String category,
                             double averageLevel, int ratingsCount, double deviation, String source) {}
