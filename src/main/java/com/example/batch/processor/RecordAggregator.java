package com.example.batch.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Aggregates a batch (e.g., 100 lines) into a single String.
 */
@Slf4j
@Component
public class RecordAggregator implements ItemProcessor<java.util.List<String>, String> {
  @Override
  public String process(List<String> items) {
    if (items == null || items.isEmpty()) return null;
    String out = String.join("\n", items);
    log.debug("Aggregated {} lines into {} chars", items.size(), out.length());
    return out;
  }
}
