def get_metric_to_weight_map():
    mapping = {
        "total_task_duration_ms": 0.6,
        "execution_duration_ms": 0.28,
        "compilation_duration_ms": 0.02,
        "read_files": 0.02,
        "read_bytes": 0.02,
        "read_rows": 0.02,
        "produced_rows": 0.02,
        "written_bytes": 0.02,
    }

    total_weight = sum(mapping.values())
    if total_weight != 1.0:
        raise ValueError(
            f"The total metrics weight must be 1.0, but it is {total_weight}"
        )

    return mapping
