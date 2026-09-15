import marimo

__generated_with = "0.24.2"
app = marimo.App(width="wide")


@app.cell
def _():
    import os
    import re
    import sys
    from pathlib import Path

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from scipy.stats import spearmanr
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.metrics import mean_absolute_error
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    import marimo as mo

    # Project root on sys.path so the notebook reads the same data layout
    # (and env overrides) as the pipeline, without importing the pipeline.
    try:
        ROOT = Path(__file__).resolve().parent.parent
    except NameError:
        ROOT = Path.cwd()
    sys.path.insert(0, str(ROOT))
    import config

    DATA_DIR = config.DATA_DIR
    return (
        ColumnTransformer,
        DATA_DIR,
        HistGradientBoostingRegressor,
        OneHotEncoder,
        Pipeline,
        mean_absolute_error,
        mo,
        np,
        os,
        pd,
        plt,
        re,
        spearmanr,
    )


@app.cell
def _(mo):
    mo.md(
        r"""
        # Signal ablation: is there signal beyond recent form?

        **Question under test:** are our features giving us enough *signal* to
        predict — or is the model mostly parroting recent points? Critically:
        **how does it predict WITHOUT knowing `points_last_3`?**

        Theory says trees shrug off useless columns (so we never bothered
        pruning), but that is not the same as having signal. This notebook
        ablates form away step by step and watches ranking + error collapse
        (or not).

        **Protocol (mirrors `model.py`, read-only — nothing here touches `data/`):**

        - Same paired `X_<gw>`/`y_<gw>` gameweeks, same `minutes_last_3`
          row filter, same one-hot + HGB pipeline shape.
        - Leave-one-gameweek-out backtest over every paired gameweek.
        - One fixed estimator for all HGB arms: the production rank champ
          (`max_depth=2`, heavy L2) — so any gap is features, not tuning.
        - Arms: **FULL** vs **NO_PTS3** (drop only `points_last_3`) vs
          **NO_FORM** (drop `points_last_3`, `xg_last_3`, `minutes_last_3`)
          vs **FORM_ONLY** (just those three) vs two naive baselines
          (train-mean, sort-by-`points_last_3`).

        **Run it:** `venv311/bin/marimo edit experiments/signal_ablation.py`
        No search here, so it runs in ~1 minute.
        """
    )
    return


@app.cell
def _(DATA_DIR, mo, os, pd, re):
    def _minutes_threshold(gameweek):
        # Mirrors model._minutes_threshold: 60% of available minutes in the
        # rolling last-3-game window.
        window = min(max(gameweek - 1, 1), 3)
        return round(0.6 * window * 90)

    def _chronological(gws):
        # Mirrors model._chronological_gws: prior-season tail (gw >= 20) first.
        gws = sorted(set(int(g) for g in gws))
        return [g for g in gws if g >= 20] + [g for g in gws if g < 20]

    pattern = re.compile(r"^(X|y)_(\d+)\.csv$")
    file_map = {}
    for f in os.listdir(DATA_DIR):
        m = pattern.match(f)
        if m:
            file_map.setdefault(int(m.group(2)), {})[m.group(1)] = f
    pairs = {gw: p for gw, p in file_map.items() if "X" in p and "y" in p}

    # train_frames skips minute-filtered-empty GWs (as model.py does);
    # test_frames falls back to unfiltered rows so early GWs still evaluate.
    # The minutes filter stays for ROW SELECTION in every arm — even arms
    # that drop minutes columns as FEATURES.
    train_frames, test_frames = {}, {}
    for gw in sorted(pairs):
        X = pd.read_csv(DATA_DIR / pairs[gw]["X"])
        y = pd.read_csv(DATA_DIR / pairs[gw]["y"])
        merged = X.merge(y, on="full_name", how="inner")
        filt = merged[merged["minutes_last_3"] >= _minutes_threshold(gw)].copy()
        test_frames[gw] = filt if not filt.empty else merged.copy()
        if not filt.empty:
            train_frames[gw] = filt

    test_gws = _chronological(list(pairs))
    CATEGORICAL = ["team_name", "player_position"]
    DROP = ["gw_points", "gw_minutes", "full_name", "gameweek"]
    _sample = next(iter(train_frames.values()))
    FEATURES_FULL = [c for c in _sample.columns if c not in DROP]
    NUMERIC_FULL = [c for c in FEATURES_FULL if c not in CATEGORICAL]
    TARGET = "gw_points"

    FORM_COLS = ["points_last_3", "xg_last_3", "minutes_last_3"]
    FEATURES_NO_PTS3 = [c for c in FEATURES_FULL if c != "points_last_3"]
    FEATURES_NO_FORM = [c for c in FEATURES_FULL if c not in FORM_COLS]
    FEATURES_FORM_ONLY = [c for c in FORM_COLS if c in FEATURES_FULL]
    NUMERIC_NO_PTS3 = [c for c in NUMERIC_FULL if c != "points_last_3"]
    NUMERIC_NO_FORM = [c for c in NUMERIC_FULL if c not in FORM_COLS]

    counts = pd.DataFrame(
        [
            {
                "gw": gw,
                "test_rows": len(test_frames[gw]),
                "in_train": gw in train_frames,
            }
            for gw in test_gws
        ]
    )
    n_train = sum(len(f) for f in train_frames.values())
    mo.vstack(
        [
            mo.md(
                f"### Data: {n_train} training rows across "
                f"{len(train_frames)} gameweeks; {len(test_gws)} test folds."
            ),
            mo.ui.table(counts),
            mo.md(
                f"FULL has {len(FEATURES_FULL)} features "
                f"({len(NUMERIC_FULL)} numeric + {len(CATEGORICAL)} one-hot). "
                f"NO_PTS3 drops 1, NO_FORM drops {len(FEATURES_FULL) - len(FEATURES_NO_FORM)}, "
                f"FORM_ONLY keeps just {FEATURES_FORM_ONLY}."
            ),
        ]
    )
    return (
        CATEGORICAL,
        FEATURES_FORM_ONLY,
        FEATURES_FULL,
        FEATURES_NO_FORM,
        FEATURES_NO_PTS3,
        FORM_COLS,
        NUMERIC_FULL,
        NUMERIC_NO_FORM,
        NUMERIC_NO_PTS3,
        TARGET,
        counts,
        test_frames,
        test_gws,
        train_frames,
    )


@app.cell
def _(mo, np, pd, plt, train_frames):
    _all = pd.concat(train_frames.values(), ignore_index=True)
    _corr_pts3 = float(_all["points_last_3"].corr(_all["gw_points"]))
    _corr_xg3 = float(_all["xg_last_3"].corr(_all["gw_points"]))
    fig_c, _ax = plt.subplots(figsize=(7, 3.5))
    _ax.scatter(
        _all["points_last_3"], _all["gw_points"], alpha=0.15, s=8, label="points_last_3"
    )
    _ax.set_xlabel("points_last_3")
    _ax.set_ylabel("actual next-gameweek points")
    _ax.set_title("Form vs target: how much does recent points even correlate?")
    mo.vstack(
        [
            fig_c,
            mo.md(
                f"Pearson r: `points_last_3` **{_corr_pts3:.3f}**, "
                f"`xg_last_3` **{_corr_xg3:.3f}**. Single-week points are "
                "noisy, so expect weak linear signal — the ablation below "
                "tests whether the model extracts anything nonlinear beyond it."
            ),
        ]
    )
    return


@app.cell
def _(
    ColumnTransformer,
    OneHotEncoder,
    Pipeline,
    mean_absolute_error,
    np,
    pd,
    spearmanr,
):
    # Production rank-champ hyperparams: the "model" step of model.predict.
    # Fixed for every HGB arm so gaps are features, not tuning.
    PROD_PARAMS = dict(
        learning_rate=0.009930253316028368,
        max_depth=2,
        max_iter=496,
        min_samples_leaf=18,
        l2_regularization=3.4019540836694557,
        random_state=42,
    )

    def make_pipeline(estimator_cls, numeric, categorical):
        from sklearn.ensemble import HistGradientBoostingRegressor

        transformers = []
        if categorical:
            transformers.append(
                ("cat", OneHotEncoder(handle_unknown="ignore"), categorical)
            )
        transformers.append(("num", "passthrough", numeric))
        return Pipeline(
            [
                ("prep", ColumnTransformer(transformers)),
                ("model", estimator_cls(**PROD_PARAMS)),
            ]
        )

    def _scores(actual, pred, names):
        rho = spearmanr(actual, pred).statistic
        overlap = len(
            set(names[np.argsort(-pred)[:20]]) & set(names[np.argsort(-actual)[:20]])
        )
        return {
            "mae": float(mean_absolute_error(actual, pred)),
            "rmse": float(np.sqrt(np.mean((actual - pred) ** 2))),
            "spearman": float(rho) if np.isfinite(rho) else 0.0,
            "top20": int(overlap),
        }

    def logo_evaluate(arms, train_frames, test_frames, test_gws, target):
        """arms: name -> dict(kind='hgb', features, numeric, categorical)
        or dict(kind='mean') or dict(kind='sort_by_pts3')."""
        from sklearn.ensemble import HistGradientBoostingRegressor

        rows = []
        per_gw = {name: [] for name in arms}
        for gw in test_gws:
            others = [g for g in test_gws if g != gw and g in train_frames]
            train = pd.concat([train_frames[g] for g in others], ignore_index=True)
            test = test_frames[gw]
            actual = test[target].to_numpy()
            names = test["full_name"].to_numpy()
            train_mean = float(train[target].mean())
            for name, spec in arms.items():
                if spec["kind"] == "hgb":
                    pipe = make_pipeline(
                        HistGradientBoostingRegressor,
                        spec["numeric"],
                        spec["categorical"],
                    )
                    pipe.fit(train[spec["features"]], train[target])
                    pred = np.asarray(pipe.predict(test[spec["features"]]))
                elif spec["kind"] == "mean":
                    pred = np.full(len(test), train_mean)
                elif spec["kind"] == "sort_by_pts3":
                    pred = test["points_last_3"].to_numpy(dtype=float)
                else:
                    raise ValueError(spec["kind"])
                s = _scores(actual, pred, names)
                s["gw"] = gw
                s["n"] = len(test)
                s["arm"] = name
                rows.append(s)
                per_gw[name].append(s)
        return pd.DataFrame(rows), per_gw

    logo_evaluate
    return PROD_PARAMS, logo_evaluate


@app.cell
def _(
    CATEGORICAL,
    FEATURES_FORM_ONLY,
    FEATURES_FULL,
    FEATURES_NO_FORM,
    FEATURES_NO_PTS3,
    NUMERIC_FULL,
    NUMERIC_NO_FORM,
    NUMERIC_NO_PTS3,
    TARGET,
    logo_evaluate,
    mo,
    test_frames,
    test_gws,
    train_frames,
):
    ARMS = {
        "FULL": {
            "kind": "hgb",
            "features": FEATURES_FULL,
            "numeric": NUMERIC_FULL,
            "categorical": CATEGORICAL,
        },
        "NO_PTS3": {
            "kind": "hgb",
            "features": FEATURES_NO_PTS3,
            "numeric": NUMERIC_NO_PTS3,
            "categorical": CATEGORICAL,
        },
        "NO_FORM": {
            "kind": "hgb",
            "features": FEATURES_NO_FORM,
            "numeric": NUMERIC_NO_FORM,
            "categorical": CATEGORICAL,
        },
        "FORM_ONLY": {
            "kind": "hgb",
            "features": FEATURES_FORM_ONLY,
            "numeric": FEATURES_FORM_ONLY,
            "categorical": [],
        },
        "MEAN": {"kind": "mean"},
        "SORT_BY_PTS3": {"kind": "sort_by_pts3"},
    }
    long_df, _per_gw = logo_evaluate(
        ARMS, train_frames, test_frames, test_gws, TARGET
    )
    summary = (
        long_df.groupby("arm")[["mae", "rmse", "spearman", "top20"]]
        .mean(numeric_only=True)
        .reindex(list(ARMS))
        .reset_index()
    )
    mo.vstack(
        [
            mo.md(
                "### Ablation result: mean over all leave-one-gameweek-out folds "
                "(SORT_BY_PTS3 predicts raw last-3 points, so read its MAE as "
                "mismeasured scale — its Spearman/Top-20 are the fair columns)."
            ),
            mo.ui.table(summary),
        ]
    )
    return ARMS, long_df, summary


@app.cell
def _(long_df, mo, np, pd, plt):
    _order = ["FULL", "NO_PTS3", "NO_FORM", "FORM_ONLY", "MEAN", "SORT_BY_PTS3"]
    fig_ab, _axes = plt.subplots(1, 2, figsize=(11, 3.5))
    _piv_mae = long_df.pivot_table(index="gw", columns="arm", values="mae")
    _piv_sp = long_df.pivot_table(index="gw", columns="arm", values="spearman")
    for _arm in [a for a in _order if a in _piv_mae.columns]:
        _axes[0].plot(_piv_mae.index.astype(str), _piv_mae[_arm], "o-", label=_arm)
        _axes[1].plot(_piv_sp.index.astype(str), _piv_sp[_arm], "s-", label=_arm)
    _axes[0].set_xlabel("held-out gameweek")
    _axes[0].set_ylabel("MAE (lower better)")
    _axes[0].legend(fontsize=8)
    _axes[1].set_xlabel("held-out gameweek")
    _axes[1].set_ylabel("Spearman (higher better)")
    _axes[1].legend(fontsize=8)
    fig_ab.tight_layout()

    _gw_table = long_df.pivot_table(
        index="gw", columns="arm", values=["mae", "spearman"]
    ).round(3)
    mo.vstack(
        [
            mo.md("### Per-gameweek: where does removing form actually hurt?"),
            fig_ab,
            mo.ui.table(_gw_table.reset_index()),
        ]
    )
    return


@app.cell
def _(long_df, mo, pd):
    _m = long_df.groupby("arm")[["mae", "spearman", "top20"]].mean()
    _full = _m.loc["FULL"]
    _nopts3 = _m.loc["NO_PTS3"]
    _noform = _m.loc["NO_FORM"]
    _formonly = _m.loc["FORM_ONLY"]
    _mean = _m.loc["MEAN"]
    _sort = _m.loc["SORT_BY_PTS3"]

    _mae_cost_pts3 = float(_nopts3["mae"] - _full["mae"])
    _rank_cost_pts3 = float(_full["spearman"] - _nopts3["spearman"])
    _mae_cost_noform = float(_noform["mae"] - _full["mae"])
    _rank_cost_noform = float(_full["spearman"] - _noform["spearman"])
    _sp = long_df.pivot_table(index="gw", columns="arm", values="spearman")
    _wins_pts3 = (
        int((_sp["FULL"] > _sp["NO_PTS3"]).sum()) if "NO_PTS3" in _sp.columns else 0
    )
    _n = len(_sp)

    if _noform["spearman"] <= _mean["spearman"] + 0.01:
        _signal = (
            "Without form the model collapses to the mean baseline — "
            "the remaining features carry **no usable ranking signal** on "
            "their own at this data size."
        )
    elif _rank_cost_noform < 0.02:
        _signal = (
            "Even with all form removed the model ranks almost as well — "
            "the non-form features (xG rates, ICT, fixture/team strength, "
            "cost) carry **real standalone signal**."
        )
    else:
        _signal = (
            "Removing all form costs real ranking power, but the model stays "
            "clear of the mean baseline — form helps, yet the other features "
            "still contribute **partial signal**."
        )

    mo.md(
        f"### Verdict\n\n"
        f"- FULL: MAE **{_full['mae']:.3f}**, Spearman **{_full['spearman']:.3f}**, "
        f"Top-20 **{_full['top20']:.2f}**/20\n"
        f"- NO_PTS3: MAE {_nopts3['mae']:.3f} ({_mae_cost_pts3:+.3f}), "
        f"Spearman {_nopts3['spearman']:.3f} ({-_rank_cost_pts3:+.3f}), "
        f"Top-20 {_nopts3['top20']:.2f} — FULL ranks better in {_wins_pts3}/{_n} folds\n"
        f"- NO_FORM: MAE {_noform['mae']:.3f} ({_mae_cost_noform:+.3f}), "
        f"Spearman {_noform['spearman']:.3f}, Top-20 {_noform['top20']:.2f}\n"
        f"- FORM_ONLY: MAE {_formonly['mae']:.3f}, Spearman {_formonly['spearman']:.3f}, "
        f"Top-20 {_formonly['top20']:.2f} — what three form columns alone can do\n"
        f"- MEAN: Spearman {_mean['spearman']:.3f} (chance level); "
        f"SORT_BY_PTS3: Spearman {_sort['spearman']:.3f}, "
        f"Top-20 {_sort['top20']:.2f} (is the model smarter than just sorting by form?)\n\n"
        f"{_signal}\n\n"
        "Read the critical question directly off NO_PTS3: that row *is* "
        "'how would we predict without knowing points_last_3'. If its "
        "Spearman/Top-20 holds near FULL, the rest of the sheet compensates; "
        "if it falls to FORM_ONLY/SORT levels, recent points were load-bearing."
    )
    return
