package com.example.batch.config;

import com.example.batch.listener.BatchLoggingListener;
import com.example.batch.partition.FileLineRangePartitioner;
import com.example.batch.processor.RecordAggregator;
import com.example.batch.reader.PartitionBatchReader;
import com.example.batch.writer.ApiItemWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Optional;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfig {

  @Value("${app.csv.file-path}")
  private String csvPath;

  @Value("${app.csv.has-header:true}")
  private boolean hasHeader;

  @Value("${app.batch.grid-size:4}")
  private int gridSize;

  private final PartitionBatchReader reader;
  private final RecordAggregator processor;
  private final ApiItemWriter writer;
  private final BatchLoggingListener listener;

  @Bean
  public Partitioner filePartitioner() {
    return new FileLineRangePartitioner(csvPath, hasHeader, gridSize);
  }

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor t = new ThreadPoolTaskExecutor();
    t.setCorePoolSize(gridSize);
    t.setMaxPoolSize(gridSize);
    t.setThreadNamePrefix("partition-");
    t.initialize();
    return t;
  }

  @Bean
  public Step slaveStep(JobRepository repo, PlatformTransactionManager tx) {
    return new StepBuilder("slaveStep", repo)
        .<java.util.List<String>, String>chunk(1, tx)
        .reader(reader)
        .processor(processor)
        .writer(writer)
        .listener(Optional.ofNullable(listener))
        .build();
  }

  @Bean
  public Step masterStep(JobRepository repo, TaskExecutor taskExecutor, Partitioner filePartitioner, Step slaveStep) {
    return new StepBuilder("masterStep", repo)
        .partitioner(slaveStep.getName(), filePartitioner)
        .step(slaveStep)
        .gridSize(gridSize)
        .taskExecutor(taskExecutor)
        .build();
  }

  @Bean
  public Job job(JobRepository repo, Step masterStep) {
    return new JobBuilder("csvPartitionJob", repo)
        .listener(listener)
        .start(masterStep)
        .build();
  }
}
