"""Top-N precision in model eval covers both top-20 and top-40."""

import pandas as pd

import model


def _frame(predicted_order):
    # predicted_order[0] = player predicted best. Higher score = better.
    n = len(predicted_order)
    return pd.DataFrame({
        "full_name": [f"Player {i}" for i in range(n)],
        # Actual rank: Player 0 is best, Player n-1 worst.
        "actual_points": list(range(n, 0, -1)),
        "predicted_points": [float(n - predicted_order.index(i)) for i in range(n)],
    })


def test_perfect_ranking_hits_both():
    df = _frame(list(range(50)))
    m = model._evaluate(df, gameweek=1, verbose=False)
    assert m["top20_precision"] == "20/20"
    assert m["top40_precision"] == "40/40"


def test_reversed_ranking_misses_top20_partially_hits_top40():
    df = _frame(list(reversed(range(50))))
    m = model._evaluate(df, gameweek=1, verbose=False)
    assert m["top20_precision"] == "0/20"
    # Predicted top-40 = actual ranks 11-50; actual top-40 = ranks 1-40.
    assert m["top40_precision"] == "30/40"
