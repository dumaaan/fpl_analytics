"""Every metric the dashboard shows: label, help text, and number format, plus
the column set, chart axes and percentile profile for each position page."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Metric:
    key: str
    label: str
    help: str
    fmt: str = "%.2f"
    percent: bool = False  # stored as 0-1, shown as 0-100%
    higher_is_better: bool = True
    text: bool = False


_METRICS = [
    Metric("total_points", "Pts", "Total FPL points this season.", "%d"),
    Metric("games_played", "GP", "Games played (1+ minutes). Every rate uses this sample.", "%d"),
    Metric("minutes", "Mins", "Minutes played this season.", "%d"),
    Metric("xpts_next_gw", "xPts next GW",
           "Projected points for the next gameweek. Uses per-90 rates, start rate, availability and "
           "the fixture model. 0 means a blank gameweek. Empty below 270 minutes.", "%.1f"),
    Metric("xpts_next_3gw", "xPts next 3 GW", "Projected points for the next 3 gameweeks.", "%.1f"),
    Metric("xpts_per_90", "xPts/90",
           "Expected points per 90 minutes from xG, xA, xG conceded, saves, DefCon rate, bonus and "
           "cards, with this season's scoring for the position."),
    Metric("points_per_90", "Pts/90", "Actual points per 90 minutes."),
    Metric("luck_per_90", "Luck/90",
           "Actual points per 90 minus expected points per 90. Large positive values often fall later.",
           "%+.2f", higher_is_better=False),
    Metric("vapm", "VAPM",
           "Value Added per Million: points above the automatic appearance points, per game played, "
           "per £1m of price."),
    Metric("save_points_per_90", "Save pts/90", "Points from saves (1 per 3 saves) and penalty saves, per 90."),
    Metric("expected_goals_conceded_per_90", "xGC/90",
           "Expected goals conceded per 90 while on the pitch. Lower is better.", higher_is_better=False),
    Metric("goals_prevented", "Goals prevented",
           "xG conceded minus goals conceded while on the pitch. Measures keeper and defence together. "
           "Large values often move back toward 0.", "%+.1f"),
    Metric("clean_sheet_rate_60", "CS rate", "Clean sheets per game of 60+ minutes.", "%.0f%%", percent=True),
    Metric("cs_expected_next_gw", "CS chance next GW",
           "Chance of a clean sheet next gameweek from the fixture model. A double gameweek adds both "
           "chances, so it can be above 100%.", "%.0f%%", percent=True),
    Metric("start_rate", "Start rate", "Starts per team match this season.", "%.0f%%", percent=True),
    Metric("sixty_minute_rate", "60+ rate", "Starts that lasted 60+ minutes (needed for clean-sheet points).",
           "%.0f%%", percent=True),
    Metric("bonus_per_90", "Bonus/90", "Bonus points per 90."),
    Metric("defcon_points_per_90", "DefCon pts/90", "Points from Defensive Contribution per 90."),
    Metric("defcon_hit_rate_adj", "DefCon rate",
           "Share of games at the DefCon threshold (DEF 10 CBIT, MID/FWD 12 CBIRT). Small samples are "
           "pulled toward the position average.", "%.0f%%", percent=True),
    Metric("defcon_near_miss_rate", "Near misses",
           "Share of games that ended 1-2 actions below the DefCon threshold. Shows upside.",
           "%.0f%%", percent=True),
    Metric("xgi_per_90", "xGI/90", "Expected goal involvement (xG + xA) per 90."),
    Metric("xg_per_90", "xG/90", "Expected goals per 90. FPL xG includes penalties."),
    Metric("xa_per_90", "xA/90", "Expected assists per 90."),
    Metric("xg_share", "xG share", "Player xG as a share of team xG in the team's matches this season.",
           "%.0f%%", percent=True),
    Metric("goals_minus_xg", "G − xG", "Goals minus expected goals. Large gaps often close later.", "%+.1f"),
    Metric("assists_minus_xa", "A − xA", "Assists minus expected assists.", "%+.1f"),
    Metric("return_rate", "Return rate", "Share of games played with a goal or an assist.", "%.0f%%", percent=True),
    Metric("haul_rate", "Haul rate", "Share of games played with 10+ points.", "%.0f%%", percent=True),
    Metric("blank_rate", "Blank rate", "Share of games played with 2 points or less.", "%.0f%%",
           percent=True, higher_is_better=False),
    Metric("ceiling", "Ceiling", "90th percentile of points in games played. Useful for captain picks.", "%.0f"),
    Metric("xgi_trend", "xGI trend", "xGI per 90 in the last 4 gameweeks minus the season xGI per 90.", "%+.2f"),
    Metric("minutes_per_xgi", "Mins/xGI", "Minutes per 1.0 expected goal involvement. Lower is better.",
           "%.0f", higher_is_better=False),
    Metric("xg_next_3gw", "xG next 3 GW",
           "Projected xG for the next 3 gameweeks: xG/90 × start rate × fixture attack strength.", "%.2f"),
    Metric("threat_per_90", "Threat/90", "FPL threat score per 90. The best API proxy for shots.", "%.0f"),
    Metric("creativity_per_90", "Creativity/90", "FPL creativity score per 90. Proxy for chances created.", "%.0f"),
    Metric("set_pieces", "Set pieces", "Penalty, corner and direct free-kick order.", text=True),
    Metric("price_change_tonight_pct", "Price change progress",
           "FPL's projection for tonight's price update. +100% means a rise, −100% a fall.", "%.0f%%"),
]

METRICS: dict[str, Metric] = {m.key: m for m in _METRICS}

POSITION_NAMES = {"GK": "Goalkeepers", "DEF": "Defenders", "MID": "Midfielders", "FWD": "Forwards"}

POSITION_INTROS = {
    "GK": "Keepers score from clean sheets, saves and bonus. Look for a high clean-sheet chance and save points.",
    "DEF": "Defenders score from clean sheets, DefCon, attacking returns and bonus.",
    "MID": "Midfielders score from goals (5), assists (3) and bonus. Some add DefCon and 1-point clean sheets.",
    "FWD": "Forwards score almost only from goals (4), assists (3) and bonus. Minutes and xG share matter most.",
}

POSITION_TABLE_COLUMNS = {
    "GK": ["total_points", "xpts_next_gw", "xpts_per_90", "save_points_per_90", "expected_goals_conceded_per_90",
           "goals_prevented", "clean_sheet_rate_60", "cs_expected_next_gw", "start_rate", "bonus_per_90", "vapm"],
    "DEF": ["total_points", "xpts_next_gw", "xpts_per_90", "defcon_points_per_90", "defcon_hit_rate_adj",
            "defcon_near_miss_rate", "expected_goals_conceded_per_90", "clean_sheet_rate_60", "cs_expected_next_gw",
            "xgi_per_90", "sixty_minute_rate", "set_pieces", "vapm"],
    "MID": ["total_points", "xpts_next_gw", "xpts_per_90", "xg_per_90", "xa_per_90", "xg_share", "goals_minus_xg",
            "assists_minus_xa", "return_rate", "haul_rate", "ceiling", "defcon_hit_rate_adj", "xgi_trend",
            "set_pieces", "vapm"],
    "FWD": ["total_points", "xpts_next_gw", "xpts_per_90", "xg_per_90", "minutes_per_xgi", "xg_share", "xg_next_3gw",
            "goals_minus_xg", "haul_rate", "ceiling", "start_rate", "bonus_per_90", "set_pieces", "vapm"],
}

POSITION_PROFILE = {
    "GK": ["xpts_per_90", "save_points_per_90", "expected_goals_conceded_per_90", "goals_prevented",
           "clean_sheet_rate_60", "bonus_per_90", "start_rate", "vapm"],
    "DEF": ["xpts_per_90", "defcon_points_per_90", "defcon_hit_rate_adj", "expected_goals_conceded_per_90",
            "clean_sheet_rate_60", "xgi_per_90", "bonus_per_90", "sixty_minute_rate", "vapm"],
    "MID": ["xpts_per_90", "xg_per_90", "xa_per_90", "xg_share", "return_rate", "haul_rate",
            "defcon_hit_rate_adj", "bonus_per_90", "vapm"],
    "FWD": ["xpts_per_90", "xg_per_90", "xa_per_90", "xg_share", "haul_rate", "bonus_per_90", "start_rate",
            "minutes_per_xgi", "vapm"],
}

# (x, y) for the position quadrant chart.
POSITION_QUADRANT = {
    "GK": ("expected_goals_conceded_per_90", "save_points_per_90"),
    "DEF": ("xgi_per_90", "defcon_hit_rate_adj"),
    "MID": ("xg_per_90", "xa_per_90"),
    "FWD": ("price", "xg_per_90"),
}

QUADRANT_NOTES = {
    "GK": "Right is worse defence (more xG conceded); up is more save points. Top-left keepers get both.",
    "DEF": "Right is more attacking threat; up is more DefCon. Top-right defenders score both ways.",
    "MID": "Right is more goal threat; up is more assist threat.",
    "FWD": "Up is more goal threat. Compare forwards at the same price.",
}

# Sequential "fixture run" signal for each position: attackers care about
# goals for, defenders and keepers about clean sheets.
FIXTURE_SIGNAL = {
    "GK": ("cs_prob", "defence_ease", "exp. clean sheets"),
    "DEF": ("cs_prob", "defence_ease", "exp. clean sheets"),
    "MID": ("xg_for", "attack_ease", "exp. team goals"),
    "FWD": ("xg_for", "attack_ease", "exp. team goals"),
}
