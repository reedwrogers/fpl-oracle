"""Understat client — player per-90 stats and team xG rates."""

import pandas as pd
from understatapi import UnderstatClient

import config


def _get_player_data(season: str) -> list[dict]:
    with UnderstatClient() as understat:
        return understat.league(league="EPL").get_player_data(season=season)


def get_player_stats(season: str = config.SEASON) -> pd.DataFrame:
    """Per-90 player stats for a season, filtered by playing-time threshold."""
    df = pd.DataFrame(_get_player_data(season))
    if df.empty:
        return df

    for col in ["time", "games", "xG", "xA", "yellow_cards", "red_cards"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["playing_time_min_percentage"] = (
        (df["time"] / (df["games"] * 90)) * 100
    ).round(2)
    df["xg_per_90"] = ((df["xG"] / df["time"]) * 90).round(2)
    df["xag_per_90"] = ((df["xA"] / df["time"]) * 90).round(2)
    df["yellows_per_90"] = ((df["yellow_cards"] / df["time"]) * 90).round(2)
    df["reds_per_90"] = ((df["red_cards"] / df["time"]) * 90).round(2)

    df = df[df["playing_time_min_percentage"] >= config.PLAYING_TIME_MIN_PCT]
    df["player_name"] = df["player_name"].replace(config.NAME_MAP)

    return df[
        ["player_name", "playing_time_min_percentage", "xg_per_90",
         "xag_per_90", "yellows_per_90", "reds_per_90"]
    ]


def get_team_stats(
    season: str = config.SEASON,
    teams: list[str] | None = None,
) -> pd.DataFrame:
    """Team xG for/against per 90 across the season so far."""
    teams = teams or config.UNDERSTAT_TEAMS

    rows = []
    today = pd.Timestamp.now()

    for team_name in teams:
        try:
            with UnderstatClient() as understat:
                team_data = understat.team(team=team_name).get_match_data(season=season)

            df = pd.DataFrame(team_data)
            if df.empty:
                continue

            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df[df["datetime"] < today]
            df = df[df["isResult"] == True]
            if len(df) == 0:
                continue

            df["team_xg"] = df.apply(
                lambda r: float(r["xG"]["h"]) if r["side"] == "h" else float(r["xG"]["a"]),
                axis=1,
            )
            df["team_xg_against"] = df.apply(
                lambda r: float(r["xG"]["a"]) if r["side"] == "h" else float(r["xG"]["h"]),
                axis=1,
            )

            total_minutes = len(df) * 90
            rows.append({
                "team_name": team_name,
                "team_xg_per_90": (df["team_xg"].sum() / total_minutes * 90).round(2),
                "team_xg_against_per_90": (df["team_xg_against"].sum() / total_minutes * 90).round(2),
                "matches_played": len(df),
            })
        except Exception as e:
            print(f"Error processing {team_name}: {e}")

    return pd.DataFrame(rows)
