CREATE TABLE IF NOT EXISTS videos (
    id            UUID PRIMARY KEY,
    url           TEXT         NOT NULL,
    title         VARCHAR(255) NOT NULL,
    category      VARCHAR(64)  NOT NULL,
    owner_id      UUID         NOT NULL,
    ratings_count INT          NOT NULL DEFAULT 0,
    rating_sum    INT          NOT NULL DEFAULT 0,   -- сумма оценок (A1=1 ... C2=6)
    created_at    TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS video_ratings (
    video_id   UUID     NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    user_id    UUID     NOT NULL,
    level      SMALLINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (video_id, user_id)                  -- один пользователь оценивает видео только один раз
);
