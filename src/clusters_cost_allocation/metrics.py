all_query_metrics = [
    "total_duration_ms",
    "execution_duration_ms",
    "compilation_duration_ms",
    "total_task_duration_ms",
    "result_fetch_duration_ms",
    "read_partitions",
    "pruned_files",
    "read_files",
    "read_rows",
    "produced_rows",
    "read_bytes",
    "spilled_local_bytes",
    "written_bytes",
    "shuffle_read_bytes",
]

# mapping of query metrics to weights
weights = {
    "total_task_duration_ms": 0.6,
    "execution_duration_ms": 0.28,
    "compilation_duration_ms": 0.02,
    "read_files": 0.02,
    "read_bytes": 0.02,
    "read_rows": 0.02,
    "produced_rows": 0.02,
    "written_bytes": 0.02,
}
