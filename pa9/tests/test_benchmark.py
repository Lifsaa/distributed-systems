from src.benchmark import split_trials


def test_split_trials_distributes_remainder_to_early_chunks():
    assert split_trials(10, 4) == [3, 3, 2, 2]


def test_split_trials_removes_zero_sized_chunks():
    assert split_trials(3, 8) == [1, 1, 1]
