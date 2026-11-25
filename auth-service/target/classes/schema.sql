CREATE TABLE IF NOT EXISTS users (
    id                UUID PRIMARY KEY,
    username          VARCHAR(64)  NOT NULL UNIQUE,
    password_hash     VARCHAR(100) NOT NULL,
    favorite_category VARCHAR(64)  NOT NULL,
    language_level    VARCHAR(2)   NOT NULL
);
