
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from typing import Tuple


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame.from_records(records)


def scale_features(df: pd.DataFrame, feature_cols: list[str]) -> tuple[pd.DataFrame, StandardScaler]:
    scaler = StandardScaler()
    df_scaled = df.copy()
    df_scaled[feature_cols] = scaler.fit_transform(df_scaled[feature_cols])
    return df_scaled, scaler


def window_sequences(
    df: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    seq_len: int,
) -> Tuple[np.ndarray, np.ndarray]:
    X, y = [], []
    values = df[feature_cols].values
    targets = df[target_col].values
    for i in range(len(df) - seq_len):
        X.append(values[i : i + seq_len])
        y.append(targets[i + seq_len])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)
