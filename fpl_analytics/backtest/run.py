"""`poetry run fpl-backtest`: score the projection model and the transfer
advisor against a season that already happened, and write a report."""

import argparse
from datetime import datetime, timezone

import pandas as pd

from fpl_analytics.backtest.data import Season, load_current_season, load_external_season
from fpl_analytics.backtest.evaluate import (
    accuracy_table,
    brier_score,
    by_position_table,
    calibration_table,
    collect_predictions,
    minutes_table,
)
from fpl_analytics.backtest.projectors import PROJECTORS
from fpl_analytics.backtest.replay import replay
from fpl_analytics.config import BACKTEST_DIR, MIN_MINUTES_FOR_PER_90

MIN_REPLAY_GWS = 6
COLUMN_NOTES = {
    "mae": "average size of the error, in points (lower is better)",
    "rmse": "like MAE, but big misses count more (lower is better)",
    "bias": "average projected minus actual (0 is best; negative = too low)",
    "rank_corr": "how well the order of players matches reality, per gameweek and position (1 = perfect)",
    "top10_capture": "points of the top 10 projected per position, as a share of the real top 10 (1 = perfect)",
    "captain_pts": "real points of the single top-projected player each gameweek",
    "best_captain_pts": "real points of the best player each gameweek (the ceiling)",
}


def _markdown(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes("number").columns:
        shown[col] = shown[col].map(lambda v: "" if pd.isna(v) else (f"{v:,.{digits}f}".rstrip("0").rstrip(".")
                                                                     if isinstance(v, float) else f"{v:,}"))
    header = "| " + " | ".join(map(str, shown.columns)) + " |"
    rule = "| " + " | ".join("---" for _ in shown.columns) + " |"
    rows = ["| " + " | ".join(map(str, row)) + " |" for row in shown.itertuples(index=False)]
    return "\n".join([header, rule, *rows])


def _findings(accuracy: pd.DataFrame, calibration: pd.DataFrame, minutes: pd.DataFrame,
              summary: pd.DataFrame | None) -> list[str]:
    notes = []
    all_h1 = accuracy[(accuracy["phase"] == "all") & (accuracy["horizon"] == "h1")].set_index("projector")
    if {"model", "model_v1"} <= set(all_h1.index):
        new, old = all_h1.loc["model"], all_h1.loc["model_v1"]
        notes.append(
            f"Model vs its first version: error {old['mae']:.2f} -> {new['mae']:.2f}, ranking "
            f"{old['rank_corr']:.2f} -> {new['rank_corr']:.2f}, captain {old['captain_pts']:.1f} -> "
            f"{new['captain_pts']:.1f} points."
        )
    if "model" in all_h1.index and len(all_h1) > 1:
        baselines = all_h1.drop(index=[p for p in ["model", "model_v1"] if p in all_h1.index])
        best_mae = baselines["mae"].idxmin()
        best_rank = baselines["rank_corr"].idxmax()
        notes.append(
            f"Next-gameweek error: model MAE {all_h1.at['model', 'mae']:.2f} vs best baseline "
            f"({best_mae}) {baselines.at[best_mae, 'mae']:.2f}. Ranking: model {all_h1.at['model', 'rank_corr']:.2f} "
            f"vs best baseline ({best_rank}) {baselines.at[best_rank, 'rank_corr']:.2f}."
        )
    regular = accuracy[(accuracy["phase"] == "regular starters") & (accuracy["horizon"] == "h3")].set_index("projector")
    if {"model", "model_v1"} <= set(regular.index):
        notes.append(
            f"Regular starters (60+ minutes last gameweek), who decide most transfers: 3-gameweek ranking "
            f"{regular.at['model_v1', 'rank_corr']:.2f} (first version) -> {regular.at['model', 'rank_corr']:.2f}. "
            "Gains here matter more for transfers than gains across all players."
        )
    early = accuracy[(accuracy["phase"].str.startswith("early")) & (accuracy["horizon"] == "h1")].set_index("projector")
    later = accuracy[(accuracy["phase"].str.startswith("later")) & (accuracy["horizon"] == "h1")].set_index("projector")
    if "model" in early.index and "model" in later.index:
        notes.append(
            f"Early season is harder: model ranking {early.at['model', 'rank_corr']:.2f} up to GW10 "
            f"vs {later.at['model', 'rank_corr']:.2f} after."
        )
    if not calibration.empty:
        low, high = calibration.iloc[0], calibration.iloc[-1]
        notes.append(
            f"Calibration: the lowest 10% of projections average {low['projected']:.2f} but score {low['actual']:.2f}; "
            f"the highest 10% average {high['projected']:.2f} but score {high['actual']:.2f}. "
            + ("Projections are too spread out: pull them toward the average."
               if low["actual"] - low["projected"] > 0.25 and high["projected"] - high["actual"] > 0.25 else "")
        )
    if not minutes.empty:
        low = minutes.iloc[0]
        notes.append(
            f"Minutes: players given a {low['predicted']:.0%} chance to appear (bucket {low['play_prob_bucket']}) "
            f"appeared {low['actual']:.0%} of the time."
        )
    if summary is not None and len(summary) > 1:
        advisors = summary[~summary["policy"].str.startswith("hold")]
        best = advisors.iloc[0]
        overlap = (advisors["highest"] >= best["lowest"]).sum() > 1
        notes.append(
            f"Replay: best mean is '{best['policy']}' with {best['mean_points']:.0f} points "
            f"({best['vs_hold']:+.0f} vs holding). "
            + ("The thresholds' ranges overlap across starting squads, so the data does not yet separate them."
               if overlap else "Its range does not overlap the other thresholds.")
        )
    return notes


def build_report(season: Season, start_gw: int, end_gw: int, predictions: pd.DataFrame, accuracy: pd.DataFrame,
                 positions: pd.DataFrame, calibration: pd.DataFrame, minutes: pd.DataFrame, brier: float,
                 summary: pd.DataFrame | None, starts: int) -> str:
    lines = [
        f"# Backtest: {season.name}",
        "",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}. Gameweeks {start_gw}–{end_gw}. "
        f"{len(predictions):,} player-gameweeks: players with {MIN_MINUTES_FOR_PER_90}+ minutes before each deadline.",
        "",
        "Every projection uses only matches before the deadline. Injury news does not exist in the history, "
        "so the model treats every player as available.",
        "",
        "## Key findings",
        "",
        *[f"- {note}" for note in _findings(accuracy, calibration, minutes, summary)],
        "",
        "## Projection accuracy",
        "",
        "h1 = the next gameweek; h3 = the next 3 gameweeks. `model` is the current model and `model_v1` its "
        "first version. Baselines: `points_per_game` (season points per match × fixtures) and `form_4gw` "
        "(last 4 gameweeks).",
        "",
        *[f"- **{col}**: {note}" for col, note in COLUMN_NOTES.items()],
        "",
        _markdown(accuracy),
        "",
        "## Next gameweek, by position",
        "",
        _markdown(positions.drop(columns=["horizon"])),
        "",
        "## Calibration of the model (next gameweek)",
        "",
        "Players split into 10 equal groups by projected points. Good calibration: projected ≈ actual in every row.",
        "",
        _markdown(calibration),
        "",
        "## Minutes model",
        "",
        f"The model's chance to appear is its chance of a 60+ minute match plus its chance of a substitute "
        f"appearance. Brier score {brier:.3f} (0 is perfect; 0.25 is a coin flip).",
        "",
        _markdown(minutes),
        "",
    ]
    if summary is not None:
        lines += [
            "## Decision replay",
            "",
            f"Each policy plays every gameweek from {starts} starting squads built from the most-owned players, "
            "under FPL's budget, selling-price, 3-per-club, free-transfer and hit rules, with automatic "
            "substitutions. Chips are not played. `advisor, X xPts/GW` makes a move when it gains at least X "
            "projected points per gameweek over the next 3 gameweeks after hits.",
            "",
            _markdown(summary, digits=1),
            "",
        ]
    lines += [
        "## Data notes",
        "",
        "- Past-season data comes from github.com/vaastav/Fantasy-Premier-League (personal use; not committed).",
        "- The dataset's `xP` column is not a pre-deadline prediction (it is near 0 for players who then did "
        "not play), so it is not used as a baseline.",
        "- FPL fixture difficulty and team strength in the files are end-of-season values, a small look-ahead "
        "in the fixture model's starting estimate.",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest the projection model and the transfer advisor.")
    parser.add_argument("--season", default="2025-26",
                        help="A folder in data/external (for example 2025-26), or 'current' for this season.")
    parser.add_argument("--start-gw", type=int, default=4, help="First gameweek to project (default 4).")
    parser.add_argument("--end-gw", type=int, default=None, help="Last gameweek (default: last finished).")
    parser.add_argument("--starts", type=int, default=5, help="Starting squads for the decision replay.")
    parser.add_argument("--no-replay", action="store_true", help="Skip the decision replay.")
    args = parser.parse_args()

    season = load_current_season() if args.season == "current" else load_external_season(args.season)
    end_gw = min(args.end_gw or season.last_complete_gw, season.last_complete_gw)
    print(f"Backtesting {season.name}: gameweeks {args.start_gw}-{end_gw}")

    names = list(PROJECTORS)
    predictions = collect_predictions(season, args.start_gw, end_gw, names) if end_gw >= args.start_gw else pd.DataFrame()
    if predictions.empty:
        print(f"Not enough finished gameweeks: no player has {MIN_MINUTES_FOR_PER_90}+ minutes before a finished "
              "gameweek yet. Try again after more gameweeks, or use --season 2025-26.")
        return

    accuracy = accuracy_table(predictions, names)
    positions = by_position_table(predictions, names)
    calibration = calibration_table(predictions)
    minutes = minutes_table(predictions)
    brier = brier_score(predictions)

    summary = weekly = None
    replay_start = max(args.start_gw, 5)
    if not args.no_replay and end_gw - replay_start + 1 >= MIN_REPLAY_GWS:
        print("Replaying decisions...")
        summary, weekly = replay(season, replay_start, end_gw, starts=args.starts)

    out_dir = BACKTEST_DIR / season.name
    out_dir.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(out_dir / "predictions.csv", index=False)
    accuracy.to_csv(out_dir / "accuracy.csv", index=False)
    positions.to_csv(out_dir / "accuracy_by_position.csv", index=False)
    calibration.to_csv(out_dir / "calibration.csv", index=False)
    minutes.to_csv(out_dir / "minutes.csv", index=False)
    if summary is not None:
        summary.to_csv(out_dir / "replay_summary.csv", index=False)
        weekly.to_csv(out_dir / "replay_weekly.csv", index=False)
    report = build_report(season, args.start_gw, end_gw, predictions, accuracy, positions, calibration, minutes,
                          brier, summary, args.starts)
    (out_dir / "report.md").write_text(report)
    print(report)
    print(f"\nWrote {out_dir}/report.md and CSV files.")


if __name__ == "__main__":
    main()
