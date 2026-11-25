-- Локальные копии данных других сервисов (обновляются событиями из Kafka)
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id           UUID PRIMARY KEY,
    favorite_category VARCHAR(64) NOT NULL,
    language_level    SMALLINT    NOT NULL           -- A1=1 ... C2=6
);

CREATE TABLE IF NOT EXISTS videos_replica (
    video_id      UUID PRIMARY KEY,
    url           TEXT,
    title         VARCHAR(255),
    category      VARCHAR(64),
    avg_level     DOUBLE PRECISION NOT NULL DEFAULT 0,
    ratings_count INT              NOT NULL DEFAULT 0
);

-- Видео считается просмотренным, если пользователь поставил ему оценку
CREATE TABLE IF NOT EXISTS watched (
    user_id  UUID NOT NULL,
    video_id UUID NOT NULL,
    PRIMARY KEY (user_id, video_id)
);

-- Идемпотентность: идентификаторы уже обработанных событий Kafka
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);
