package com.example.batch.partition;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Partitions a single CSV file by line counts.
 * Each partition receives:
 *  - startLine: number of lines to skip from beginning (including header)
 *  - itemsToRead: number of lines to read in this partition
 */
public class FileLineRangePartitioner implements Partitioner {

  private final String filePath;
  private final boolean hasHeader;
  private final int gridSize;

  public FileLineRangePartitioner(String filePath, boolean hasHeader, int gridSize) {
    this.filePath = filePath;
    this.hasHeader = hasHeader;
    this.gridSize = gridSize;
  }

  @Override
  public Map<String, ExecutionContext> partition(int ignored) {
    try {
      long totalLines = Files.lines(Path.of(filePath)).count();
      long header = hasHeader ? 1 : 0;
      long dataLines = Math.max(0, totalLines - header);

      int parts = Math.max(1, gridSize);
      long base = dataLines / parts;
      long remainder = dataLines % parts;

      Map<String, ExecutionContext> map = new HashMap<>();
      long cursor = header; // skip header globally

      for (int i = 0; i < parts; i++) {
        long size = base + (i < remainder ? 1 : 0);
        ExecutionContext ctx = new ExecutionContext();
        ctx.putLong("startLine", cursor);
        ctx.putLong("itemsToRead", size);
        ctx.putString("partition", "partition" + i);
        map.put("partition" + i, ctx);
        cursor += size;
      }
      return map;
    } catch (IOException e) {
      throw new RuntimeException("Failed to partition file " + filePath, e);
    }
  }
}
