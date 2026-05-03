# Summer 2026 — FPL Oracle Roadmap

## Phase 1: Finish the 2025–26 Season

### Data Collection (already running)
- `data_v3.py` runs each gameweek via cron, producing `X_{gw}.csv` (before GW) and `y_{gw-1}.csv` (after GW)
- By season end we should have ~38 gameweeks of paired X/y data

### Review & Backtest
- Run `model.py` against every past gameweek to see how metrics trend over the season
- Identify gameweeks where the model performed poorly and investigate:
  - Was it missing features (injuries, suspensions, rotation)?
  - Did the Understat threshold filter out too many players early in the season?
  - Were there name-matching failures that dropped players from the dataset?

### Model Tweaks to Explore

**Handling low-minute players early in the season**
- The current `pt_threshold=60` (60% minutes played) filters aggressively early on
- Options to try:
  - Dynamic threshold — start lower (e.g., 30%) for early GWs, ramp up as season progresses
  - Separate "established" vs "emerging" models — one for regulars, one for rotation players
  - Use rolling window minutes instead of season-wide (last 5 GWs)

**Handling promoted/new teams**
- Promoted Championship sides won't be in the prior season's Understat data
- Fallback approaches:
  - Use Championship Understat data as a prior, with a decay weight as PL data accumulates
  - Estimate team strength from FPL API (team name + league position, which is available)
  - Use betting market odds or pre-season predictions as a proxy for team xG/xGA

**Feature engineering ideas**
- Form decay: weight recent gameweeks more heavily in `points_last_3`, `xg_last_3`
- Opponent strength: incorporate rolling form of opponent rather than season averages
- Captaincy/bandwagons: ownership % changes can indicate public sentiment (contrarian bet?)
- Injury flags: scrape injury news or use minutes trends as a proxy
- Double gameweeks / blank gameweeks: this is huge for FPL

**Model architecture**
- Try XGBoost / LightGBM vs HistGradientBoosting
- Try calibrating predictions with isotonic regression (better point estimates)
- Try predicting *minutes* separately from *points-per-minute* (two-stage model)
- Ensemble of models trained on different feature subsets

---

## Phase 2: Transfer Optimizer (Integer Programming)

### Goal
Given a predicted point total for every player for the next `n` gameweeks, select the optimal squad that maximizes expected points subject to FPL rules.

### What We Maximize
**Total expected points over the planning horizon** (e.g., next 5–10 gameweeks). This accounts for:
- Future fixtures (a player with easy fixtures gets higher expected points in later weeks)
- Transfers used (each transfer costs -4 pts, so the optimizer should only transfer if the gain exceeds the cost)

### Constraints

**Squad selection (15 players)**

| Constraint | Detail |
|---|---|
| Total players | Exactly 15 |
| Goalkeepers | Exactly 2 |
| Defenders | Exactly 5 |
| Midfielders | Exactly 5 |
| Forwards | Exactly 3 |
| Per team | Max 3 players from any single club |
| Budget | Total cost ≤ 100.0 |

**Starting XI (each gameweek)**

| Constraint | Detail |
|---|---|
| Formation | 1 GK / 3–5 DEF / 3–5 MID / 1–3 FWD |
| Captain | Exactly 1 (points ×2) |
| Vice-captain | Exactly 1 (fills in if captain doesn't play) |
| Bench | 4 players, order matters (auto-substitution rules) |

**Transfers (rolling between gameweeks)**

| Constraint | Detail |
|---|---|
| Free transfers | 1 per gameweek (can roll up to 1 extra, so max 2 stored) |
| Hit cost | -4 pts per additional transfer beyond free allowance |
| Transfers affect budget | Selling price = 50% of (purchase price + current price) — simplification may be needed |

### Implementation Plan

1. **Predict rolling n gameweeks**: run the model for each upcoming GW using fixture data to get expected points for every player × GW combination
2. **Build the optimization model** using `pulp` or `ortools`:
   - Decision variables: `x_{p}` = 1 if player p is in squad, 0 otherwise (15 total)
   - Decision variables: `start_{p,gw}` = 1 if player p starts in GW, 0 otherwise
   - Decision variables: `captain_{p,gw}` = 1 if player p is captain in GW, 0 otherwise
   - Objective: `maximize sum(points_{p,gw} * start_{p,gw} + points_{p,gw} * captain_{p,gw})` (captain gets doubled)
   - Add all FPL constraints as linear inequalities
3. **Incremental approach**: start simple (single GW, ignore transfer costs), then add complexity:
   - Multi-GW with transfer costs
   - Bench order / auto-subs
   - Player price changes
   - Chip usage (wildcard, free hit, bench boost, triple captain)

### Why This Order
Solving a single-GW team selection is easy. Adding the temporal dimension (transfers, future fixtures, budget carryover) is where it gets interesting. We'll build in layers so we have something working from day one of the 2026–27 season.
