import numpy as np
import pandas as pd

from fpl_analytics.config import MIN_MINUTES_FOR_PER_90, SHRINK_PRIOR_GAMES


def per_90(stat: pd.Series, minutes: pd.Series) -> pd.Series:
    """Rate per 90 minutes. Empty (NaN) below MIN_MINUTES_FOR_PER_90, so a
    one-minute cameo can't "project" to an absurd rate, and small samples
    show as missing rather than as a real 0 that ranks and plots."""
    return pd.Series(
        np.where(minutes >= MIN_MINUTES_FOR_PER_90, stat * 90 / minutes.where(minutes > 0), np.nan),
        index=stat.index,
    )


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """numerator / denominator, NaN where the denominator is 0."""
    return numerator / denominator.where(denominator > 0)


def shrunk_rate(successes: pd.Series, trials: pd.Series, prior_rate: pd.Series | float,
                prior_trials: float = SHRINK_PRIOR_GAMES) -> pd.Series:
    """(successes + k * prior) / (trials + k): pulls small samples toward the prior."""
    return (successes + prior_trials * prior_rate) / (trials + prior_trials)
