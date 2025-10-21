package com.example.batch.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchController {

  private final JobLauncher jobLauncher;
  private final Job csvPartitionJob;
  private final JobExplorer jobExplorer;

  @PostMapping("/trigger")
  public Map<String, Object> trigger() throws Exception {
    String jobName = csvPartitionJob.getName();
    if (!jobExplorer.findRunningJobExecutions(jobName).isEmpty()) {
      return Map.of("message", "Job is already running", "jobName", jobName);
    }
    JobParameters params = new JobParametersBuilder()
        .addLong("timestamp", System.currentTimeMillis())
        .toJobParameters();
    jobLauncher.run(csvPartitionJob, params);
    return Map.of("message", "Job started", "jobName", jobName);
  }

  @GetMapping("/status")
  public Map<String, Object> status() {
    String jobName = csvPartitionJob.getName();
    JobInstance last = jobExplorer.getLastJobInstance(jobName);
    if (last == null) return Map.of("status", "NEVER_RAN", "jobName", jobName);
    JobExecution exec = jobExplorer.getLastJobExecution(last);
    Map<String, Object> resp = new LinkedHashMap<>();
    resp.put("jobName", jobName);
    resp.put("status", exec.getStatus().toString());
    resp.put("startTime", exec.getStartTime());
    resp.put("endTime", exec.getEndTime());
    List<Map<String, Object>> steps = new ArrayList<>();
    for (StepExecution se : exec.getStepExecutions()) {
      Map<String, Object> s = new LinkedHashMap<>();
      s.put("stepName", se.getStepName());
      s.put("status", se.getStatus().toString());
      s.put("readCount", se.getReadCount());
      s.put("writeCount", se.getWriteCount());
      s.put("commitCount", se.getCommitCount());
      s.put("startLine", se.getExecutionContext().getLong("startLine", -1L));
      s.put("itemsToRead", se.getExecutionContext().getLong("itemsToRead", -1L));
      steps.add(s);
    }
    resp.put("steps", steps);
    return resp;
  }
}
