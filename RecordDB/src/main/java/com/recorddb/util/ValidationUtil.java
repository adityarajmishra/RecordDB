package com.recorddb.util;

import java.util.Map;
import java.util.regex.Pattern;

public class ValidationUtil {
    private static final Pattern KEY_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");
    private static final Pattern VALUE_PATTERN = Pattern.compile("[^,{}]+");

    public static boolean validateDocument(Map<String, String> document) {
        // Check if document is null or empty
        if (document == null || document.isEmpty()) {
            return false;
        }

        // Check if _id exists and is not empty
        if (!document.containsKey("_id") || document.get("_id").trim().isEmpty()) {
            return false;
        }

        // Validate each key-value pair
        return document.entrySet().stream()
                .allMatch(entry ->
                        // Key must not be null and must match pattern
                        entry.getKey() != null &&
                                KEY_PATTERN.matcher(entry.getKey()).matches() &&
                                // Value must not be null, empty, and must match pattern
                                entry.getValue() != null &&
                                !entry.getValue().trim().isEmpty() &&
                                VALUE_PATTERN.matcher(entry.getValue()).matches());
    }
}
