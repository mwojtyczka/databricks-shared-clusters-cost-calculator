import math


def get_metric_to_weight_map():
    mapping = {
        "total_task_duration_ms": 0.5,
        "execution_duration_ms": 0.3,
        "compilation_duration_ms": 0.05,
        "read_files": 0.03,
        "read_bytes": 0.03,
        "read_rows": 0.03,
        "produced_rows": 0.03,
        "written_bytes": 0.03,
    }

    total_weight = sum(mapping.values())
    if not math.isclose(total_weight, 1.0, rel_tol=1e-9):
        raise ValueError(
            f"The total metrics weight must be 1.0, but it is {total_weight}"
        )

    return mapping
