import numpy as np
import pandas as pd


def per_90(stat: pd.Series, minutes: pd.Series) -> pd.Series:
    return np.where(minutes > 0, stat * 90 / minutes, 0.0)


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return np.where(denominator > 0, numerator / denominator, 0.0)
