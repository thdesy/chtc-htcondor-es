
ElasticSearch upload for HTCondor data
----------------------------------------------

This package contains a set of scripts that assist in uploading data from
a HTCondor pool to ElasticSearch.  It queries for both historical and current
job ClassAds, converts them to a JSON document, and uploads them to the
local ElasticSearch instance.

The majority of the logic is in the conversion of ClassAds to values that
are immediately useful in Kibana / ElasticSearch.

Important Attributes
--------------------

This service converts the raw ClassAds into JSON documents; one JSON document
per job in the system.  While most attributes are copied over verbatim, a few
new ones are computed on insertion; this allows easier construction of
ElasticSearch queries.   This section documents the most commonly used and
most useful attributes, along with their meaning.

Generic attributes:
- `RecordTime`: When the job exited the queue or when the JSON document was last
  updated, whichever came first.  Use this field for time-based queries.
- `GlobalJobId`: Use to uniquely identify individual jobs.
- `BenchmarkJobDB12`: An estimate of the per-core performance of the machine the job last
  ran on, based on the `DB12` benchmark.  Higher is better.
- `CoreHr`: The number of core-hours utilized by the job.  If the job lasted for
  24 hours and utilized 4 cores, the value of `CoreHr` will be 96.  This includes all
  runs of the job, including any time spent on preempted instance.
- `CpuTimeHr`: The amount of CPU time (sum of user and system) attributed to the job,
  in hours.
- `CommittedCoreHr`: The core-hours only for the last run of the job (excluding preempted
  attempts).
- `QueueHrs`: Number of hours the job spent in queue before last run.
- `WallClockHr`: Number of hours the job spent running.  This is invariant of the
  number of cores; most users will prefer `CoreHr` instead.
- `CpuEff`: The total scheduled CPU time (user and system CPU) divided by core hours,
  in percentage.  If the job lasted for 24 hours, utilized 4 cores, and used 72 hours
  of CPU time, then `CpuEff` would be 75.
- `CpuBadput`: The badput associated with a job, in hours.  This is the sum of all
  unsuccessful job attempts.  If a job runs for 2 hours, is preempted, restarts,
  then completes successfully after 3 hours, the `CpuBadput` is 2.
- `MemoryMB`: The amount of RAM used by the job.
- `Processor`: The processor model (from `/proc/cpuinfo`) the job last ran on.
- `RequestCpus`: Number of cores utilized by the job.
- `RequestMemory`: Amount of memory requested by the job, in MB.
- `ScheddName`: Name of HTCondor schedd where the job ran.
- `Status`: State of the job; valid values include `Completed`, `Running`, `Idle`,
  or `Held`.  *Note*: due to some validation issues, non-experts should only look
  at completed jobs.
- `x509userproxysubject`: The DN of the grid certificate associated with the job; for
  CMS jobs, this is not the best attribute to use to identify a user (prefer `CRAB_UserHN`).
- `DB12CommittedCoreHr`, `DB12CoreHr`, `DB12CpuTimeHr`: The job's `CommittedCoreHr`,
  `CoreHr`, and `CpuTimeHr` values multiplied by the `BenchmarkJobDB12` score.

## Configuration

```plain
usage: spider_cms.py [-h] [--process_queue] [--feed_es] [--feed_es_for_queues]
                     [--feed_amq] [--schedd_filter SCHEDD_FILTER]
                     [--skip_history] [--read_only] [--dry_run]
                     [--max_documents_to_process MAX_DOCUMENTS_TO_PROCESS]
                     [--keep_full_queue_data]
                     [--es_bunch_size ES_BUNCH_SIZE]
                     [--query_queue_batch_size QUERY_QUEUE_BATCH_SIZE]
                     [--upload_pool_size UPLOAD_POOL_SIZE]
                     [--query_pool_size QUERY_POOL_SIZE]
                     [--es_hostname ES_HOSTNAME] [--es_port ES_PORT]
                     [--es_index_template ES_INDEX_TEMPLATE]
                     [--log_dir LOG_DIR] [--log_level LOG_LEVEL]
                     [--email_alerts EMAIL_ALERTS] [--collectors COLLECTORS]
                     [--collectors_file COLLECTORS_FILE]

```

The collectors file is a json file with the pools and a list of the collectors: 

```json
{
    "Global":[ "thefirstcollector.example.com:8080","thesecondcollector.other.com"],
    "Volunteer":["next.example.com"],
    "ITB":["newcollector.virtual.com"]
}
```
