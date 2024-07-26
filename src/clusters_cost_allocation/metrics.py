import math


def get_metric_to_weight_map() -> dict[str, float]:
    """
    Get mapping of query metrics to weights.
    @return:
    """
    # Total task duration (total_task_duration_ms) gives a good estimation of the cluster CPU utilization,
    # therefore, plays a very important role in the cost calculation, especially for larger distributed queries.
    # It’s important to include total duration (total_duration_ms) and compilation (compilation_duration_ms)
    # metrics in the calculation because small driver side queries and cached queries won’t have any task duration.
    # But since larger queries can complete relatively quickly if the cluster is autoscaled,
    # compilation and execution time can skew the calculation and therefore should have smaller weight
    # than total task duration.
    # The remaining 15% of the contribution comes from metrics related to IO operations which play some role
    # in cost contribution but overall are more important for storage cost calculation.
    #
    # Note that total query duration (total_duraction_ms) metric is not useful in the cost calculation
    # because it is heavily influenced by scaling activities (waiting time).
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
