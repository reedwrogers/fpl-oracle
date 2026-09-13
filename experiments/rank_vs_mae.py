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
    import optuna
    import pandas as pd
    from scipy.stats import spearmanr
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import Ridge
    from sklearn.metrics import mean_absolute_error
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    import marimo as mo

    optuna.logging.set_verbosity(optuna.logging.WARNING)

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
        RandomForestRegressor,
        Ridge,
        mean_absolute_error,
        mo,
        np,
        optuna,
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
        # Rank vs MAE: are they the same model?

        **Question under test:** does a model tuned to *rank players correctly*
        (who scores big?) end up being the same model as one tuned to *minimize
        point error* (MAE)? Spoiler from theory: usually **no** — most players
        blank each week, so MAE is dominated by the 0–2pt mass and rewards models
        that hug the mean. Ranking rewards models that stick their neck out on
        haulers, even if that costs absolute accuracy. This notebook checks that
        empirically on our own data.

        **Protocol (mirrors `model.py`, read-only — nothing here touches `data/`):**

        - Same paired `X_<gw>`/`y_<gw>` gameweeks, same `minutes_last_3` filter,
          same one-hot + HGB pipeline shape.
        - Leave-one-gameweek-out backtest over all 11 paired gameweeks
          (prior-season tail first, then current season).
        - Study A minimizes **mean MAE**; Study B maximizes a **rank metric**
          (Spearman, or Top-20 overlap via the dropdown). Same search space,
          same seed, same folds — the objective is the only thing that differs.

        **Run it:** `venv311/bin/marimo edit experiments/rank_vs_mae.py`
        (extra deps, already installed in `venv311`: `marimo`, `optuna`,
        `matplotlib`). Default 20 trials/objective takes several minutes; raise
        the slider for a more thorough search, lower it for a quick smoke run.
        Both studies share a sampler seed on purpose (paired design: identical
        candidates while TPE warms up, then each adapts to its own objective).
        """
    )
    return


@app.cell
def _(mo):
    n_trials = mo.ui.slider(5, 50, 5, value=20, label="Optuna trials per objective")
    seed = mo.ui.slider(1, 100, 1, value=42, label="Random seed (shared by both studies)")
    rank_target = mo.ui.dropdown(
        {"Spearman correlation": "spearman", "Top-20 overlap": "top20"},
        value="Spearman correlation",
        label="Rank objective to maximize",
    )
    return n_trials, rank_target, seed


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
    FEATURES = [c for c in _sample.columns if c not in DROP]
    NUMERIC = [c for c in FEATURES if c not in CATEGORICAL]
    TARGET = "gw_points"

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
                f"{len(train_frames)} gameweeks; {len(test_gws)} test folds. "
                "GWs missing from train (all-zero minute histories) still "
                "evaluate as test folds — same as `model.predict`."
            ),
            mo.ui.table(counts),
        ]
    )
    return (
        CATEGORICAL,
        FEATURES,
        NUMERIC,
        TARGET,
        counts,
        test_frames,
        test_gws,
        train_frames,
    )


@app.cell
def _(mo, np, pd, plt, train_frames):
    _all_points = pd.concat(train_frames.values(), ignore_index=True)["gw_points"]
    fig_dist, _ax = plt.subplots(figsize=(7, 3.5))
    _ax.hist(_all_points, bins=range(int(_all_points.max()) + 2), edgecolor="white")
    _ax.set_xlabel("actual gameweek points")
    _ax.set_ylabel("players")
    _ax.set_title("Target distribution: the 0–2pt mass is what MAE mostly sees")
    _blank_share = float((_all_points <= 2).mean() * 100)
    mo.vstack(
        [
            fig_dist,
            mo.md(
                f"{_blank_share:.1f}% of training rows score ≤2pts. "
                "A model that always predicts ~2pts gets a respectable MAE "
                "while saying nothing about who hauls — keep this in mind "
                "when the two studies disagree."
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
    # Production hyperparams: the "model" step of model.predict.
    PROD_PARAMS = dict(
        learning_rate=0.01, max_depth=4, max_iter=200, min_samples_leaf=20
    )

    def make_pipeline(estimator, numeric, categorical, impute=False):
        prep = ColumnTransformer(
            [
                ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
                ("num", "passthrough", numeric),
            ]
        )
        steps = [("prep", prep)]
        if impute:
            # Trees handle NaN natively; linear models need median imputation.
            from sklearn.impute import SimpleImputer

            steps.append(("imp", SimpleImputer(strategy="median")))
        steps.append(("model", estimator))
        return Pipeline(steps)

    def fold_scores(
        train_list, test_frame, features, target, numeric, categorical, estimator, impute=False
    ):
        train = pd.concat(train_list, ignore_index=True)
        pipe = make_pipeline(estimator, numeric, categorical, impute=impute)
        pipe.fit(train[features], train[target])
        pred = np.asarray(pipe.predict(test_frame[features]))
        actual = test_frame[target].to_numpy()
        rho = spearmanr(actual, pred).statistic
        names = test_frame["full_name"].to_numpy()
        overlap = len(
            set(names[np.argsort(-pred)[:20]]) & set(names[np.argsort(-actual)[:20]])
        )
        return {
            "mae": float(mean_absolute_error(actual, pred)),
            "rmse": float(np.sqrt(np.mean((actual - pred) ** 2))),
            "spearman": float(rho) if np.isfinite(rho) else 0.0,
            "top20": int(overlap),
        }

    def logo_evaluate(
        train_frames,
        test_frames,
        test_gws,
        features,
        target,
        numeric,
        categorical,
        estimator,
        impute=False,
    ):
        rows = []
        for gw in test_gws:
            others = [g for g in test_gws if g != gw and g in train_frames]
            s = fold_scores(
                [train_frames[g] for g in others],
                test_frames[gw],
                features,
                target,
                numeric,
                categorical,
                estimator,
                impute=impute,
            )
            s["gw"] = gw
            s["n"] = len(test_frames[gw])
            rows.append(s)
        return pd.DataFrame(rows).set_index("gw").sort_index()

    logo_evaluate
    return PROD_PARAMS, fold_scores, logo_evaluate, make_pipeline


@app.cell
def _(
    CATEGORICAL,
    FEATURES,
    HistGradientBoostingRegressor,
    NUMERIC,
    PROD_PARAMS,
    TARGET,
    logo_evaluate,
    mo,
    seed,
    test_frames,
    test_gws,
    train_frames,
):
    base_est = HistGradientBoostingRegressor(**PROD_PARAMS, random_state=seed.value)
    base_df = logo_evaluate(
        train_frames, test_frames, test_gws, FEATURES, TARGET, NUMERIC, CATEGORICAL, base_est
    )
    base_mean = base_df.mean(numeric_only=True)
    mo.vstack(
        [
            mo.md(
                f"### Baseline (production params): mean MAE **{base_mean['mae']:.3f}**, "
                f"Spearman **{base_mean['spearman']:.3f}**, "
                f"Top-20 **{base_mean['top20']:.1f}/20**"
            ),
            mo.ui.table(base_df.reset_index()),
        ]
    )
    return base_df, base_mean


@app.cell
def _(
    CATEGORICAL,
    FEATURES,
    HistGradientBoostingRegressor,
    NUMERIC,
    PROD_PARAMS,
    RandomForestRegressor,
    Ridge,
    TARGET,
    logo_evaluate,
    mo,
    pd,
    seed,
    test_frames,
    test_gws,
    train_frames,
):
    # "Correct algo?" — same data + protocol, different estimators, no tuning.
    # Trees use native NaN handling; Ridge gets median imputation (no scaling),
    # so read it as "is anything linear competitive?", not a tuned linear model.
    candidates = {
        "hgb_prod": (HistGradientBoostingRegressor(**PROD_PARAMS, random_state=seed.value), False),
        "hgb_default": (HistGradientBoostingRegressor(random_state=seed.value), False),
        "rf_100": (
            RandomForestRegressor(n_estimators=100, n_jobs=-1, random_state=seed.value),
            False,
        ),
        "ridge": (Ridge(alpha=1.0), True),
    }
    algo_rows = []
    for _name, (_est, _imp) in candidates.items():
        _d = logo_evaluate(
            train_frames,
            test_frames,
            test_gws,
            FEATURES,
            TARGET,
            NUMERIC,
            CATEGORICAL,
            _est,
            impute=_imp,
        )
        algo_rows.append(
            {
                "algo": _name,
                "mae": _d["mae"].mean(),
                "rmse": _d["rmse"].mean(),
                "spearman": _d["spearman"].mean(),
                "top20": _d["top20"].mean(),
            }
        )
    algo_df = pd.DataFrame(algo_rows).sort_values("mae").reset_index(drop=True)
    mo.vstack(
        [
            mo.md(
                "### Algo check (untuned, same protocol). "
                "If the MAE column and the rank columns crown different winners "
                "even here, that is already evidence the objectives disagree."
            ),
            mo.ui.table(algo_df),
        ]
    )
    return (algo_df,)


@app.cell
def _(
    CATEGORICAL,
    FEATURES,
    HistGradientBoostingRegressor,
    NUMERIC,
    TARGET,
    logo_evaluate,
    mo,
    n_trials,
    optuna,
    seed,
    test_frames,
    test_gws,
    train_frames,
):
    def hgb_from_trial(trial):
        return HistGradientBoostingRegressor(
            learning_rate=trial.suggest_float("learning_rate", 1e-3, 0.3, log=True),
            max_depth=trial.suggest_categorical("max_depth", [2, 3, 4, 6, 8, None]),
            max_iter=trial.suggest_int("max_iter", 100, 500),
            min_samples_leaf=trial.suggest_int("min_samples_leaf", 5, 50),
            l2_regularization=trial.suggest_float("l2_regularization", 1e-3, 10.0, log=True),
            random_state=seed.value,
        )

    def objective_mae(trial):
        _d = logo_evaluate(
            train_frames,
            test_frames,
            test_gws,
            FEATURES,
            TARGET,
            NUMERIC,
            CATEGORICAL,
            hgb_from_trial(trial),
        )
        trial.set_user_attr("spearman", float(_d["spearman"].mean()))
        trial.set_user_attr("top20", float(_d["top20"].mean()))
        return float(_d["mae"].mean())

    study_mae = optuna.create_study(
        direction="minimize", sampler=optuna.samplers.TPESampler(seed=seed.value)
    )
    study_mae.optimize(objective_mae, n_trials=n_trials.value)
    mo.md(
        f"### Study A done: best mean MAE **{study_mae.best_value:.3f}** "
        f"with `{study_mae.best_params}`"
    )
    return hgb_from_trial, study_mae


@app.cell
def _(
    CATEGORICAL,
    FEATURES,
    NUMERIC,
    TARGET,
    hgb_from_trial,
    logo_evaluate,
    mo,
    n_trials,
    optuna,
    rank_target,
    seed,
    test_frames,
    test_gws,
    train_frames,
):
    rank_key = rank_target.value

    def objective_rank(trial):
        _d = logo_evaluate(
            train_frames,
            test_frames,
            test_gws,
            FEATURES,
            TARGET,
            NUMERIC,
            CATEGORICAL,
            hgb_from_trial(trial),
        )
        trial.set_user_attr("mae", float(_d["mae"].mean()))
        trial.set_user_attr("spearman", float(_d["spearman"].mean()))
        trial.set_user_attr("top20", float(_d["top20"].mean()))
        return float(_d[rank_key].mean())

    study_rank = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed.value)
    )
    study_rank.optimize(objective_rank, n_trials=n_trials.value)
    mo.md(
        f"### Study B done ({rank_key}): best **{study_rank.best_value:.3f}** "
        f"with `{study_rank.best_params}`"
    )
    return rank_key, study_rank


@app.cell
def _(mo, np, optuna, pd, plt, study_mae, study_rank):
    def _best_so_far(study):
        vals = [t.value for t in study.trials if t.value is not None]
        if study.direction == optuna.study.StudyDirection.MINIMIZE:
            return vals, list(np.minimum.accumulate(vals))
        return vals, list(np.maximum.accumulate(vals))

    fig_hist, _axes = plt.subplots(1, 2, figsize=(11, 3.5))
    for _a, _study, _ttl in zip(
        _axes, [study_mae, study_rank], ["Study A: minimize mean MAE", "Study B: maximize rank"]
    ):
        _v, _b = _best_so_far(_study)
        _a.plot(_v, ".", alpha=0.5, label="trial")
        _a.plot(_b, "-", label="best so far")
        _a.set_title(_ttl)
        _a.set_xlabel("trial")
        _a.legend()
    fig_hist.tight_layout()

    _recs = []
    for _tag, _study in [("mae-study", study_mae), ("rank-study", study_rank)]:
        for _t in _study.trials:
            if _t.value is None:
                continue
            _recs.append(
                {
                    "study": _tag,
                    "mae": _t.user_attrs.get("mae", _t.value),
                    "spearman": _t.user_attrs.get("spearman"),
                    "top20": _t.user_attrs.get("top20"),
                }
            )
    scatter = pd.DataFrame(_recs)
    fig_trade, _ax2 = plt.subplots(figsize=(7, 4.5))
    for _tag, _grp in scatter.groupby("study"):
        _ax2.scatter(_grp["mae"], _grp["spearman"], alpha=0.6, label=_tag)
    _ax2.set_xlabel("mean MAE (lower is better)")
    _ax2.set_ylabel("mean Spearman (higher is better)")
    _ax2.set_title("Every trial in MAE-vs-rank space: a frontier means tradeoff")
    _ax2.legend()
    mo.vstack(
        [
            mo.md("### Optimization traces"),
            fig_hist,
            mo.md(
                "### The tradeoff, one dot per trial. "
                "If the objectives agreed, the best-MAE dot would also be the "
                "top-most dot. If they trace a frontier instead, tuning for one "
                "costs you the other."
            ),
            fig_trade,
        ]
    )
    return (scatter,)


@app.cell
def _(
    CATEGORICAL,
    FEATURES,
    HistGradientBoostingRegressor,
    NUMERIC,
    TARGET,
    logo_evaluate,
    mo,
    np,
    pd,
    plt,
    seed,
    study_mae,
    study_rank,
    test_frames,
    test_gws,
    train_frames,
):
    mae_champ = HistGradientBoostingRegressor(**study_mae.best_params, random_state=seed.value)
    rank_champ = HistGradientBoostingRegressor(**study_rank.best_params, random_state=seed.value)
    _kw = dict(
        train_frames=train_frames,
        test_frames=test_frames,
        test_gws=test_gws,
        features=FEATURES,
        target=TARGET,
        numeric=NUMERIC,
        categorical=CATEGORICAL,
    )
    mae_df = logo_evaluate(**_kw, estimator=mae_champ)
    rank_df = logo_evaluate(**_kw, estimator=rank_champ)
    comp = pd.DataFrame(
        [
            {
                "champ": "MAE champ",
                "mae": mae_df["mae"].mean(),
                "rmse": mae_df["rmse"].mean(),
                "spearman": mae_df["spearman"].mean(),
                "top20": mae_df["top20"].mean(),
            },
            {
                "champ": "Rank champ",
                "mae": rank_df["mae"].mean(),
                "rmse": rank_df["rmse"].mean(),
                "spearman": rank_df["spearman"].mean(),
                "top20": rank_df["top20"].mean(),
            },
        ]
    )
    params_cmp = pd.DataFrame(
        [study_mae.best_params, study_rank.best_params], index=["MAE champ", "Rank champ"]
    ).T
    fig_champs, _axes2 = plt.subplots(1, 2, figsize=(11, 3.5))
    _gw_labels = [str(g) for g in mae_df.index]
    _x = np.arange(len(_gw_labels))
    _axes2[0].plot(_x, mae_df["mae"], "o-", label="MAE champ")
    _axes2[0].plot(_x, rank_df["mae"], "s-", label="Rank champ")
    _axes2[0].set_xticks(_x, _gw_labels)
    _axes2[0].set_xlabel("held-out gameweek")
    _axes2[0].set_ylabel("MAE (lower better)")
    _axes2[0].legend()
    _axes2[1].plot(_x, mae_df["spearman"], "o-", label="MAE champ")
    _axes2[1].plot(_x, rank_df["spearman"], "s-", label="Rank champ")
    _axes2[1].set_xticks(_x, _gw_labels)
    _axes2[1].set_xlabel("held-out gameweek")
    _axes2[1].set_ylabel("Spearman (higher better)")
    _axes2[1].legend()
    fig_champs.tight_layout()
    mo.vstack(
        [
            mo.md("### Showdown: each champ re-evaluated on every fold, all metrics"),
            mo.ui.table(comp),
            mo.md("### Winning hyperparameters side by side"),
            mo.ui.table(params_cmp.reset_index(names="param")),
            mo.md("### Per-gameweek: where does each champ win?"),
            fig_champs,
        ]
    )
    return comp, mae_df, params_cmp, rank_df


@app.cell
def _(comp, mae_df, mo, rank_df, study_mae, study_rank):
    _m = comp.set_index("champ")
    _same = study_mae.best_params == study_rank.best_params
    _mae_cost = _m.loc["Rank champ", "mae"] - _m.loc["MAE champ", "mae"]
    _rank_cost = _m.loc["Rank champ", "spearman"] - _m.loc["MAE champ", "spearman"]
    _rank_wins = int((rank_df["spearman"] > mae_df["spearman"]).sum())
    _mae_wins = int((mae_df["mae"] < rank_df["mae"]).sum())
    _n = len(mae_df)
    if _same:
        _closing = (
            "At this trial budget both objectives crown the **same config** — "
            "either they genuinely agree on this data, or the shared random "
            "candidates were too coarse to separate them. Raise the trial "
            "slider and watch whether the winners diverge and whether the "
            "tradeoff scatter forms a frontier. Note the algo table above "
            "already shows MAE and rank disagreeing across estimators."
        )
    else:
        _closing = (
            "Your instinct was right: **rank-optimal ≠ MAE-optimal**, and a "
            "single-MAE-tuned model is a choice with a real cost. The practical "
            "fix this experiment points at: tune/select on the rank metric (or "
            "a blend) instead of MAE alone — and consider a two-head setup (one "
            "model for expected points, one for haul/captain ranking) if the "
            "frontier is steep."
        )
    mo.md(
        f"### Verdict\n\n"
        f"- Identical winning hyperparameters? **{'YES' if _same else 'NO'}**\n"
        f"- Price of ranking well: Rank champ is **{_mae_cost:+.3f} MAE** "
        f"worse than the MAE champ.\n"
        f"- Price of accuracy: MAE champ is **{_rank_cost:+.3f} Spearman** "
        f"below the Rank champ.\n"
        f"- Per-gameweek wins: Rank champ ranks better in "
        f"**{_rank_wins}/{_n}** folds; MAE champ has lower error in "
        f"**{_mae_wins}/{_n}** folds.\n\n" + _closing
    )
    return
