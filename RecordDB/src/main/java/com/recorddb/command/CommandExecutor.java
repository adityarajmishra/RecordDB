package com.recorddb.command;

import com.recorddb.core.DatabaseOperations;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandExecutor {
    private final DatabaseOperations database;
    private static final Pattern DOCUMENT_PATTERN =
            Pattern.compile("\\{([^}]+)\\}");

    public CommandExecutor(DatabaseOperations database) {
        this.database = database;
    }

    public CompletableFuture<String> executeCommand(String command) {
        String[] parts = command.trim().split("\\s+", 2);
        String operation = parts[0].toUpperCase();

        return switch (operation) {
            case "INSERT_ONE" -> executeInsertOne(parts[1]);
            case "INSERT_MANY" -> executeInsertMany(parts[1])
                    .thenApply(results -> String.join(",", results));
            case "FIND" -> executeFind(parts[1])
                    .thenApply(results -> String.join(",", results));
            case "DELETE" -> executeDelete(parts[1])
                    .thenApply(count -> "DELETED " + count + " File(s)");
            case "STOP" -> {
                database.stop();
                yield CompletableFuture.completedFuture("Adios!");
            }
            case "PURGE_AND_STOP" -> {
                database.purgeAndStop();
                yield CompletableFuture.completedFuture("PURGED, Adios!");
            }
            default -> CompletableFuture.completedFuture("INVALID_COMMAND");
        };
    }

    private CompletableFuture<String> executeInsertOne(String documentStr) {
        return parseDocument(documentStr)
                .map(database::insertOne)
                .orElse(CompletableFuture.completedFuture("INVALID_COMMAND"));
    }

    private CompletableFuture<List<String>> executeInsertMany(String documentsStr) {
        List<Map<String, String>> documents = parseDocuments(documentsStr);
        return documents.isEmpty()
                ? CompletableFuture.completedFuture(Collections.singletonList("INVALID_COMMAND"))
                : database.insertMany(documents);
    }

    private CompletableFuture<List<String>> executeFind(String criteriaStr) {
        return parseDocument(criteriaStr)
                .map(database::find)
                .orElse(CompletableFuture.completedFuture(
                        Collections.singletonList("INVALID_COMMAND")));
    }

    private CompletableFuture<Integer> executeDelete(String criteriaStr) {
        return parseDocument(criteriaStr)
                .map(database::delete)
                .orElse(CompletableFuture.completedFuture(0));
    }

    private Optional<Map<String, String>> parseDocument(String documentStr) {
        Matcher matcher = DOCUMENT_PATTERN.matcher(documentStr);
        if (!matcher.find()) return Optional.empty();

        Map<String, String> document = new HashMap<>();
        String[] pairs = matcher.group(1).split(",");

        // Check for invalid key-value pairs
        for (String pair : pairs) {
            String[] parts = pair.trim().split(":", 2);
            // If we don't have exactly two parts (key:value), return empty
            if (parts.length != 2 || parts[1].trim().isEmpty()) {
                return Optional.empty();
            }
            document.put(parts[0].trim(), parts[1].trim());
        }

        return Optional.of(document);
    }

    private List<Map<String, String>> parseDocuments(String documentsStr) {
        List<Map<String, String>> documents = new ArrayList<>();
        Matcher matcher = DOCUMENT_PATTERN.matcher(documentsStr);

        while (matcher.find()) {
            parseDocument("{" + matcher.group(1) + "}")
                    .ifPresent(documents::add);
        }

        return documents;
    }
}
