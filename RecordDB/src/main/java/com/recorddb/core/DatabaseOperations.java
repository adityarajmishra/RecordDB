package com.recorddb.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface DatabaseOperations {
    CompletableFuture<String> insertOne(Map<String, String> document);
    CompletableFuture<List<String>> insertMany(List<Map<String, String>> documents);
    CompletableFuture<List<String>> find(Map<String, String> criteria);
    CompletableFuture<Integer> delete(Map<String, String> criteria);
    void stop();
    void purgeAndStop();
}
