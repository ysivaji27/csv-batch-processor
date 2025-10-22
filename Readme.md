Spring Batch solves this using Partitioned Steps, not “multi-threaded reading” directly.

Concept

The Master Step divides the input into partitions — e.g.:

Thread 1 → lines 1 – 2 million

Thread 2 → lines 2 000 001 – 4 million

Thread 3 → lines 4 000 001 – 6 million

etc.

Each Slave Step gets its own FlatFileItemReader,
configured to read only that section of the file.

The Master coordinates them concurrently using a TaskExecutor.
