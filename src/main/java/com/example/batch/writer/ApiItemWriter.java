package com.example.batch.writer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

@Slf4j
@Component
public class ApiItemWriter implements ItemWriter<String> {

  private final WebClient client;
  private final String path;

  public ApiItemWriter(@Value("${app.api.base-url}") String baseUrl,
                       @Value("${app.api.path:/upload}") String path) {
    this.client = WebClient.builder().baseUrl(baseUrl).build();
    this.path = path;
  }

  @Override
  public void write(Chunk<? extends String> chunk) {
    for (String payload : chunk.getItems()) {
      client.post().uri(path).bodyValue(payload).retrieve()
          .toBodilessEntity().block(Duration.ofMinutes(2));
      log.info("Sent aggregated batch ({} chars)", payload.length());
    }
  }
}
