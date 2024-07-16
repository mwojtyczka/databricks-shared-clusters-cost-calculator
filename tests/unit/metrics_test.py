from clusters_cost_allocation.metrics import get_metric_to_weight_map


def test_metric_to_weight_map_should_not_throw_exception():
    assert get_metric_to_weight_map()
