from src.ray_runner import build_task_plan


def test_build_task_plan_assigns_deterministic_seeds_to_chunks():
    task_plan = build_task_plan([5, 5, 4], base_seed=100)

    assert task_plan == [(5, 100), (5, 101), (4, 102)]
