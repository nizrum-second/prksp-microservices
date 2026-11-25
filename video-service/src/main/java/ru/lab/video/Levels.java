package ru.lab.video;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Уровни CEFR: A1=1 ... C2=6. */
final class Levels {

    private static final String[] NAMES = {"A1", "A2", "B1", "B2", "C1", "C2"};

    private Levels() {}

    static int toNumber(String level) {
        if (level != null) {
            for (int i = 0; i < NAMES.length; i++) {
                if (NAMES[i].equalsIgnoreCase(level.trim())) {
                    return i + 1;
                }
            }
        }
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "level must be one of A1,A2,B1,B2,C1,C2");
    }

    /** Среднее значение (1..6) -> ближайший уровень CEFR. */
    static String toName(double average) {
        int n = (int) Math.round(average);
        return NAMES[Math.max(1, Math.min(6, n)) - 1];
    }
}
