package ru.lab.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class JwtService {

    private final Algorithm algorithm;
    private final long ttlMinutes;

    public JwtService(@Value("${jwt.secret}") String secret,
                      @Value("${jwt.ttl-minutes}") long ttlMinutes) {
        this.algorithm = Algorithm.HMAC256(secret);
        this.ttlMinutes = ttlMinutes;
    }

    public long ttlMinutes() {
        return ttlMinutes;
    }

    /** kid нужен KrakenD, чтобы найти ключ в jwk.json. */
    public String create(String userId, String jti) {
        Date now = new Date();
        return JWT.create()
                .withKeyId("k1")
                .withSubject(userId)
                .withJWTId(jti)
                .withIssuedAt(now)
                .withExpiresAt(new Date(now.getTime() + ttlMinutes * 60_000))
                .sign(algorithm);
    }

    /** Принимает значение заголовка Authorization ("Bearer xxx"). Бросает исключение, если токен плохой. */
    public DecodedJWT verify(String authorizationHeader) {
        if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
            throw new IllegalArgumentException("no bearer token");
        }
        return JWT.require(algorithm).build().verify(authorizationHeader.substring(7));
    }
}
