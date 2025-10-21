package com.example.batch.reader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads a partitioned segment of a file and returns batches of N lines as a List<String> per read().
 * It uses step execution context keys: startLine, itemsToRead.
 */
@Slf4j
@Component
public class PartitionBatchReader implements ItemStreamReader<List<String>>, ItemStream {

  private final String filePath;
  private final int batchSize;

  private BufferedReader reader;
  private long remaining;   // lines left to read in this partition

  public PartitionBatchReader(@Value("${app.csv.file-path}") String filePath,
                              @Value("${app.batch.chunk-size:100}") int batchSize) {
    this.filePath = filePath;
    this.batchSize = batchSize;
  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
    try {
      reader = new BufferedReader(new FileReader(new FileSystemResource(filePath).getFile()));
      long startLine = executionContext.getLong("startLine", 0L);
      long itemsToRead = executionContext.getLong("itemsToRead", Long.MAX_VALUE);
      this.remaining = itemsToRead;

      // Skip lines up to startLine
      for (long i = 0; i < startLine; i++) {
        if (reader.readLine() == null) break;
      }
      log.info("Reader opened at startLine={} with itemsToRead={}", startLine, itemsToRead);
    } catch (IOException e) {
      throw new ItemStreamException("Failed to open file " + filePath, e);
    }
  }

  @Override
  public List<String> read() throws Exception {
    if (remaining <= 0) return null;

    List<String> lines = new ArrayList<>(batchSize);
    String line;
    while (lines.size() < batchSize && remaining > 0 && (line = reader.readLine()) != null) {
      lines.add(line);
      remaining--;
    }
    if (lines.isEmpty()) return null;
    return lines;
  }

  @Override
  public void update(ExecutionContext executionContext) throws ItemStreamException {
    // No-op; could persist remaining if needed
  }

  @Override
  public void close() throws ItemStreamException {
    if (reader != null) {
      try { reader.close(); } catch (IOException ignore) {}
    }
  }
}
