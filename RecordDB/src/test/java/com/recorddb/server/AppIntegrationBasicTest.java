package com.recorddb.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AppIntegrationBasicTest {
    private void assertAppOutput(String input, String expectedOutput) {
        // Set up input stream
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
        System.setIn(in);

        // Set up output stream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        // Run the main method
        App.main(new String[]{});

        // Capture and process the output
        String actualOutput = out.toString().replaceAll("\\r\\n", "\n").trim();
        String formattedExpectedOutput = expectedOutput.replaceAll("\\r\\n", "\n").trim();

        if (!formattedExpectedOutput.equals(actualOutput)) {
            // Split the expected and actual outputs into lines
            String[] expectedLines = formattedExpectedOutput.split("\n");
            String[] actualLines = actualOutput.split("\n");

            // Determine the maximum length of the expected and actual output lines for proper alignment
            int maxExpectedLength = 0;
            int maxActualLength = 0;
            for (String line : expectedLines) {
                if (line.length() > maxExpectedLength) {
                    maxExpectedLength = line.length();
                }
            }
            for (String line : actualLines) {
                if (line.length() > maxActualLength) {
                    maxActualLength = line.length();
                }
            }

            // Determine the width of the header and format
            int columnWidth = Math.max(maxExpectedLength, maxActualLength) + 5;
            String expectedHeader = "===EXPECTED OUTPUT===";
            String actualHeader = "===ACTUAL OUTPUT===";
            String header = String.format("%-" + columnWidth + "s | %s", expectedHeader, actualHeader);

            // ANSI escape codes for coloring
            String redColor = "\u001B[31m";
//            String blue = "\u001B[34m";
            String magentaColor = "\u001B[35m";
            String resetColor = "\u001B[0m";

            // Build the failure message with aligned columns
            StringBuilder failureMessage = new StringBuilder();
            failureMessage.append(redColor).append("Test Failed For:\n").append(resetColor);
//            failureMessage.append(blue).append("====INPUT SEQUENCE====\n").append(input.trim()).append(resetColor).append("\n");
            failureMessage.append(magentaColor).append("====INPUT SEQUENCE====\n").append(resetColor).append(input.trim()).append("\n");
            failureMessage.append(magentaColor).append(header).append(resetColor).append("\n");

            int maxLines = Math.max(expectedLines.length, actualLines.length);
            for (int i = 0; i < maxLines; i++) {
                String expectedLine = i < expectedLines.length ? expectedLines[i] : "";
                String actualLine = i < actualLines.length ? actualLines[i] : "";

                // Add spaces to the expected line to align with the actual output column
                String formattedExpectedLine = String.format("%-" + columnWidth + "s", expectedLine);

                if (!expectedLine.equals(actualLine)) {
                    failureMessage.append(redColor).append(formattedExpectedLine).append(" | ").append(actualLine).append(resetColor).append("\n");
                } else {
                    failureMessage.append(formattedExpectedLine).append(" | ").append(actualLine).append("\n");
                }
            }

            Assertions.fail(failureMessage.toString());
        }

        // Assert equality with detailed message
        assertEquals(formattedExpectedOutput, actualOutput);
    }
    @Test
    public void testCombinedCases()  {
        String input =
                // testExample1 sequence
                "INSERT_ONE { _id: 101, employee: JohnSmith, department: HR, location: NY }\n" +
                        "INSERT_ONE { _id: 102, client: AcmeCorp, industry: Tech, revenue: 500M }\n" +
                        "FIND { employee: JohnSmith }\n" +
                        "DELETE { department: HR }\n" +
                        "FIND { employee: JohnSmith }\n" +

                        // testExample2 sequence
                        "INSERT_MANY [{ _id: 201, book: 1984, author: George Orwell, genre: Dystopia }, " +
                        "{ _id: 202, book: Brave New World, author: Aldous Huxley, genre: SciFi }, " +
                        "{ _id: 203, book: Fahrenheit 451, author: Ray Bradbury, genre: Dystopia }]\n" +
                        "FIND { genre: Dystopia }\n" +
                        "DELETE { author: George Orwell }\n" +
                        "FIND { genre: Dystopia }\n" +

                        // testExample3 sequence
                        "INSERT_ONE { _id: 301, flight: AB123, airline: Air Blue, destination: Paris, status: On Time }\n" +
                        "INSERT_ONE { _id: 302, flight: XY789, airline: Sky Fly, destination: London, status: Delayed }\n" +
                        "FIND { destination: Paris }\n" +
                        "DELETE { flight: XY789 }\n" +
                        "FIND { airline: Sky Fly }\n" +

                        // testExample4 sequence
                        "INSERT_ONE { _id: 401, product: Laptop, brand: Tech Brand, price: 1500, warranty: 2 years }\n" +
                        "INSERT_ONE { _id: 402, product: Smartphone, brand: Mobile X, price: 800, warranty: 1 year }\n" +
                        "FIND { brand: Tech Brand }\n" +
                        "INSERT_ONE { _id: 403, product: Tablet, brand: Tech Brand, price: 600, warranty }\n" +
                        "DELETE { price: 800 }\n" +
                        "FIND { brand: Mobile X }\n" +

                        // testExample5 sequence
                        "INSERT_ONE { _id: 501, movie: Inception, director: Christopher Nolan, rating: 8.8 }\n" +
                        "INSERT_MANY [{ _id: 502, movie: Interstellar, director: Christopher Nolan, rating: 8.6 }, " +
                        "{ _id: 503, movie: TheDarkKnight, director: Christopher Nolan, rating: 9.0 }]\n" +
                        "FIND { director: Christopher Nolan, rating: 9.0 }\n" +
                        "DELETE { movie: Inception }\n" +
                        "FIND { director: Christopher Nolan }\n" +

                        // PURGE_AND_STOP once at the end
                        "PURGE_AND_STOP";

        String expectedOutput =
                // Expected output for testExample1
                "SUCCESS\n" +
                        "SUCCESS\n" +
                        "101\n" +
                        "DELETED 1 File(s)\n" +
                        "NO_RECORD_AVAILABLE\n" +

                        // Expected output for testExample2
                        "SUCCESS,SUCCESS,SUCCESS\n" +
                        "201,203\n" +
                        "DELETED 1 File(s)\n" +
                        "203\n" +

                        // Expected output for testExample3
                        "SUCCESS\n" +
                        "SUCCESS\n" +
                        "301\n" +
                        "DELETED 1 File(s)\n" +
                        "NO_RECORD_AVAILABLE\n" +

                        // Expected output for testExample4
                        "SUCCESS\n" +
                        "SUCCESS\n" +
                        "401\n" +
                        "INVALID_COMMAND\n" +
                        "DELETED 1 File(s)\n" +
                        "NO_RECORD_AVAILABLE\n" +

                        // Expected output for testExample5
                        "SUCCESS\n" +
                        "SUCCESS,SUCCESS\n" +
                        "503\n" +
                        "DELETED 1 File(s)\n" +
                        "502,503\n" +

                        // PURGE_AND_STOP output
                        "PURGED, Adios!";

        assertAppOutput(input, expectedOutput);
    }

}
