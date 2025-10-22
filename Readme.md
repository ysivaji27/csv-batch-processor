# csv-batch-processor

Spring Batch project to stream a **large CSV (e.g., 15 GB)**, process in **chunks of 100**, and **POST batches to an API**.
Includes:
- Partitioned processing (multi-threaded over one big file)
    Concept
        The Master Step divides the input into partitions — e.g.:
        Thread 1 → lines 1 – 2 million
        Thread 2 → lines 2 000 001 – 4 million
        Thread 3 → lines 4 000 001 – 6 million
    etc.
        Each Slave Step gets its own FlatFileItemReader, configured to read only that section of the file.
        The Master coordinates them concurrently using a TaskExecutor.
- Progress logging listener
- REST endpoints to trigger job and check status
- Optional scheduling via cron

## Quick start

1. Update `src/main/resources/application.yml`:
   - `app.csv.file-path`
   - `app.api.base-url`
   - `app.api.path`
   - Adjust `grid-size`, `chunk-size`, and `schedule` if needed.

2. Build and run:
   ```bash
   ./mvnw spring-boot:run
   # or
   mvn spring-boot:run
   ```

3. Endpoints:
   - `POST /api/batch/trigger` → start job (if not running)
   - `GET  /api/batch/status`  → latest job status, step metrics

## Notes

- For very large files, H2 file DB is used to persist Batch metadata so the job can restart.
- Partitioning splits the file by line ranges; each partition has its own reader with `linesToSkip` and `maxItemCount`.
- The writer sends each chunk (100 records) as a JSON array to your API.

