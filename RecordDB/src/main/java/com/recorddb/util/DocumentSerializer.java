package com.recorddb.util;

import com.recorddb.model.Document;
import java.io.*;
import java.nio.file.*;
import java.util.logging.*;

public class DocumentSerializer {
    private static final Logger LOGGER = Logger.getLogger(DocumentSerializer.class.getName());

    public void serialize(Document document, String filePath) {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(Paths.get(filePath))))) {
            oos.writeObject(document);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to serialize document", e);
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public Document deserialize(String filePath) {
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(Paths.get(filePath))))) {
            return (Document) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to deserialize document", e);
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
