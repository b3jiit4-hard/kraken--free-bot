import pandas as pd
import numpy as np

class EMACrossATR:
    def __init__(self, ema_fast=20, ema_slow=50, atr_period=14, atr_k=2.5):
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.atr_period = atr_period
        self.atr_k = atr_k

    @staticmethod
    def _atr(df: pd.DataFrame, period: int) -> pd.Series:
        high = df['high']
        low = df['low']
        close = df['close']
        prev_close = close.shift(1)
        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(period, min_periods=1).mean()

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['ema_fast'] = df['close'].ewm(span=self.ema_fast, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=self.ema_slow, adjust=False).mean()
        df['atr'] = self._atr(df, self.atr_period)
        df['signal_long'] = (df['ema_fast'] > df['ema_slow']) & (df['ema_fast'].shift(1) <= df['ema_slow'].shift(1))
        df['signal_exit'] = (df['ema_fast'] < df['ema_slow']) & (df['ema_fast'].shift(1) >= df['ema_slow'].shift(1))
        df['stop_dist'] = self.atr_k * df['atr']  # distanza stop in EUR
        return df
