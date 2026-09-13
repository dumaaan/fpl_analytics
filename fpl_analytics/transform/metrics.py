import numpy as np
import pandas as pd

from fpl_analytics.config import MIN_MINUTES_FOR_PER_90


def per_90(stat: pd.Series, minutes: pd.Series) -> pd.Series:
    """Rate per 90 minutes, 0 below MIN_MINUTES_FOR_PER_90 so a one-minute
    cameo can't "project" to an absurd rate."""
    return np.where(minutes >= MIN_MINUTES_FOR_PER_90, stat * 90 / minutes, 0.0)


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return np.where(denominator > 0, numerator / denominator, 0.0)
