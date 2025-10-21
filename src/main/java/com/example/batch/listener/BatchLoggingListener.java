package com.example.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class BatchLoggingListener extends ChunkListenerSupport
        implements JobExecutionListener, ItemReadListener<Object> {

  private final AtomicLong groups = new AtomicLong();

  @Override
  public void beforeJob(JobExecution jobExecution) {
    groups.set(0);
    log.info("🚀 Job '{}' started", jobExecution.getJobInstance().getJobName());
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    log.info("✅ Job '{}' completed with status {} ({} groups processed)",
        jobExecution.getJobInstance().getJobName(), jobExecution.getStatus(), groups.get());
  }

  @Override
  public void afterRead(Object item) {
    long n = groups.incrementAndGet();
    if (n % 100 == 0) {
      log.info("Processed {} groups (each group is N lines from YAML)", n);
    }
  }

  @Override
  public void afterChunk(ChunkContext context) {
    StepExecution se = context.getStepContext().getStepExecution();
    log.debug("Chunk done. step='{}' read={} write={} commits={}",
        se.getStepName(), se.getReadCount(), se.getWriteCount(), se.getCommitCount());
  }
}
