package com.recorddb.core;

import com.recorddb.model.Document;
import com.recorddb.util.DocumentSerializer;
import com.recorddb.util.ValidationUtil;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.logging.Logger;
import java.util.logging.Level;

public class NoSQLDatabase implements DatabaseOperations {
    private static final Logger LOGGER = Logger.getLogger(NoSQLDatabase.class.getName());
    private static final String BASE_PATH = "src/main/resources/generated/";
    private final ExecutorService executorService;
    private final ReadWriteLock rwLock;
    private final ConcurrentMap<String, Document> memoryStore;
    private final DocumentSerializer serializer;
    private volatile boolean isRunning;
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    public NoSQLDatabase() {
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE, r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        });
        this.rwLock = new ReentrantReadWriteLock();
        this.memoryStore = new ConcurrentHashMap<>();
        this.serializer = new DocumentSerializer();
        this.isRunning = true;
        initializeDirectory();
    }

    private void initializeDirectory() {
        try {
            Files.createDirectories(Paths.get(BASE_PATH));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize directory", e);
            throw new RuntimeException("Database initialization failed", e);
        }
    }

    @Override
    public CompletableFuture<String> insertOne(Map<String, String> document) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                rwLock.writeLock().lock();
                if (!isRunning) throw new IllegalStateException("Database is stopped");

                if (!ValidationUtil.validateDocument(document)) {
                    return "INVALID_COMMAND";
                }

                String id = document.get("_id");
                if (memoryStore.containsKey(id)) {
                    return "ID_CONFLICT";
                }

                Document doc = new Document();
                document.forEach(doc::put);
                memoryStore.put(id, doc);

                serializer.serialize(doc, BASE_PATH + id + ".bin");
                return "SUCCESS";
            } finally {
                rwLock.writeLock().unlock();
            }
        }, executorService);
    }

    @Override
    public CompletableFuture<List<String>> insertMany(List<Map<String, String>> documents) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                rwLock.writeLock().lock();
                if (!isRunning) throw new IllegalStateException("Database is stopped");

                return documents.parallelStream()
                        .map(doc -> CompletableFuture.supplyAsync(() -> {
                            if (!ValidationUtil.validateDocument(doc)) {
                                return "INVALID_COMMAND";
                            }
                            String id = doc.get("_id");
                            if (memoryStore.containsKey(id)) {
                                return "ID_CONFLICT";
                            }
                            Document document = new Document();
                            doc.forEach(document::put);
                            memoryStore.put(id, document);
                            serializer.serialize(document, BASE_PATH + id + ".bin");
                            return "SUCCESS";
                        }, executorService))
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList());
            } finally {
                rwLock.writeLock().unlock();
            }
        }, executorService);
    }

    @Override
    public CompletableFuture<List<String>> find(Map<String, String> criteria) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                rwLock.readLock().lock();
                if (!isRunning) throw new IllegalStateException("Database is stopped");

                List<String> results = memoryStore.entrySet().parallelStream()
                        .filter(entry -> matchesCriteria(entry.getValue(), criteria))
                        .map(Map.Entry::getKey)
                        .sorted()
                        .collect(Collectors.toList());

                return results.isEmpty() ?
                        Collections.singletonList("NO_RECORD_AVAILABLE") : results;
            } finally {
                rwLock.readLock().unlock();
            }
        }, executorService);
    }

    @Override
    public CompletableFuture<Integer> delete(Map<String, String> criteria) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                rwLock.writeLock().lock();
                if (!isRunning) throw new IllegalStateException("Database is stopped");

                Set<String> toDelete = memoryStore.entrySet().parallelStream()
                        .filter(entry -> matchesCriteria(entry.getValue(), criteria))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());

                toDelete.forEach(id -> {
                    memoryStore.remove(id);
                    try {
                        Files.deleteIfExists(Paths.get(BASE_PATH + id + ".bin"));
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "Failed to delete file: " + id);
                    }
                });

                return toDelete.size();
            } finally {
                rwLock.writeLock().unlock();
            }
        }, executorService);
    }

    private boolean matchesCriteria(Document document, Map<String, String> criteria) {
        return criteria.entrySet().stream()
                .allMatch(entry ->
                        Objects.equals(document.get(entry.getKey()), entry.getValue()));
    }

    @Override
    public void stop() {
        try {
            rwLock.writeLock().lock();
            isRunning = false;
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.SEVERE, "Database shutdown interrupted", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void purgeAndStop() {
        try {
            rwLock.writeLock().lock();
            memoryStore.clear();
            try (Stream<Path> paths = Files.walk(Paths.get(BASE_PATH))) {
                paths.filter(Files::isRegularFile)
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                LOGGER.log(Level.WARNING, "Failed to delete file: " + path, e);
                            }
                        });
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to purge database", e);
        } finally {
            stop();
            rwLock.writeLock().unlock();
        }
    }
}
