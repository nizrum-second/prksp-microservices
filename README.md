# Микросервисная система подбора видео для изучения языка (итоговая практическая работа)

Фрагмент информационной системы адаптивного подбора видеоматериалов для изучения иностранного языка (Spring Boot, WebFlux, Kafka, PostgreSQL, Redis, KrakenD, Traefik). Весь стек поднимается одной командой:

```bash
docker compose up --build
```

## Архитектура

```
                         Client (curl / Postman)
                                  │  :8080
                                  ▼
                              Traefik  ◄── внешний reverse proxy (dashboard :8081)
                  ┌───────────────┴────────────────────┐
                  │ всё, кроме SSE                      │ /api/recommendations/stream (SSE)
                  ▼                                     │  + forwardAuth → auth-service (JWT + Redis)
              KrakenD  (API Gateway: JWT, маршрутизация,│
                  │     X-User-Id, X-Request-ID,        │
                  │     /api/health — агрегированный)   │
     ┌────────────┼──────────────┐                      │
     ▼            ▼              ▼                      │
 auth-service  video-service  recommendation-service ◄──┘   (WebFlux: Mono/Flux/SSE/WebClient)
  │    │          │   ▲            │   │
  │    │          │   └── HTTP (WebClient: timeout + retry + Circuit Breaker + fallback)
  ▼    ▼          ▼                ▼
auth_db Redis  video_db         rec_db  (+ processed_events — идемпотентность)
  │             │                  ▲
  └─user-registered─►  Kafka  ◄─video-events (VIDEO_CREATED/UPDATED/DELETED/RATED)
                         └────────────────────────────┘ потребитель: recommendation-service
Jaeger (трассировка, UI :16686)
```

| Сервис | Назначение | Хранилище |
|---|---|---|
| auth-service | регистрация, логин, JWT, токены в Redis | auth_db (PostgreSQL), Redis |
| video-service | каталог видео, оценки сложности A1–C2 | video_db |
| recommendation-service | рекомендации, SSE-поток, потребитель Kafka | rec_db |
