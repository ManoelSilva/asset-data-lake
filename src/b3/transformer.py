import logging
import time

import numpy as np
import pandas as pd


class B3Transformer(object):
    @staticmethod
    def transform_b3_hist_quota(df: pd.DataFrame) -> pd.DataFrame:
        start_time = time.time()
        logging.info("Transforming B3 hist quota..")
        df = df.copy()

        # Remove columns that are all null
        df = df.loc[:, df.notnull().any()]
        logging.info(f"Removed columns that are all null")
        
        # Handle missing market column by creating dummy value
        if 'market' not in df.columns:
            logging.warning("Market column not found, creating dummy market column with value '000'")
            df['market'] = '000'

        try:
            # One-hot encode 'type', 'market', 'currency' (if not too many unique values)
            for col in ['type', 'market', 'currency']:
                if df[col].nunique() < 20:
                    dummies = pd.get_dummies(df[col], prefix=col)
                    df = pd.concat([df, dummies], axis=1)
        except Exception as e:
            logging.exception(f"Exception while transforming B3 hist quota {e}")
        # Parse date
        df['date'] = pd.to_datetime(df['date'])
        # Sort for rolling features
        df = df.sort_values(['ticker', 'date'])
        # Numeric columns
        price_cols = ['open', 'high', 'low', 'avg', 'close', 'best_buy', 'best_sell']
        for col in price_cols + ['volume', 'turnover']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # Group by ticker for rolling features
        grouped = df.groupby('ticker', group_keys=False)
        # Returns
        df['close_return'] = grouped['close'].pct_change()
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
        # Moving averages
        df['ma_5'] = grouped['close'].transform(lambda x: x.rolling(5).mean())
        df['ma_10'] = grouped['close'].transform(lambda x: x.rolling(10).mean())
        # Volatility (rolling std of returns)
        df['volatility_5'] = grouped['close_return'].transform(lambda x: x.rolling(5).std())
        # Momentum (ma_5 - ma_10)
        df['momentum'] = df['ma_5'] - df['ma_10']
        # Volume features
        df['volume_5ma'] = grouped['volume'].transform(lambda x: x.rolling(5).mean())
        df['volume_change'] = grouped['volume'].pct_change()
        # Price ratios
        df['hl_ratio'] = df['high'] / df['low']
        df['co_ratio'] = df['close'] / df['open']
        df['ch_ratio'] = df['close'] / df['high']
        df['cl_ratio'] = df['close'] / df['low']
        # Turnover per volume
        df['turnover_per_vol'] = df['turnover'] / df['volume']
        # Lag features
        df['close_lag1'] = grouped['close'].shift(1)
        df['volume_lag1'] = grouped['volume'].shift(1)
        # Only keep features from the Example Feature List
        # Daily return
        df['daily_return'] = (df['close'] - df['open']) / df['open']
        # Rolling volatility 5 (std of daily_return)
        grouped = df.groupby('ticker', group_keys=False)
        df['rolling_volatility_5'] = grouped['daily_return'].transform(lambda x: x.rolling(5).std())
        # Moving average 10
        df['moving_avg_10'] = grouped['close'].transform(lambda x: x.rolling(10).mean())
        # MACD (12-26 EMA difference)
        df['ema_12'] = grouped['close'].transform(lambda x: x.ewm(span=12, adjust=False).mean())
        df['ema_26'] = grouped['close'].transform(lambda x: x.ewm(span=26, adjust=False).mean())
        df['macd'] = df['ema_12'] - df['ema_26']
        # RSI (14)
        def rsi(series, window=14):
            delta = series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
            rs = gain / loss
            return 100 - (100 / (1 + rs))
        df['rsi_14'] = grouped['close'].transform(rsi)
        # Volume change
        df['volume_change'] = grouped['volume'].pct_change()
        # Avg volume 10
        df['avg_volume_10'] = grouped['volume'].transform(lambda x: x.rolling(10).mean())
        # Best buy/sell spread
        df['best_buy_sell_spread'] = df['best_sell'] - df['best_buy']
        # Close to best buy
        df['close_to_best_buy'] = (df['close'] - df['best_buy']) / df['best_buy']
        # Market type NM one-hot
        # Handle null/empty market values by filling with dummy value
        df['market'] = df['market'].fillna('000').astype(str)
        df['market_type_NM'] = (df['market'].str.contains('NM')).astype(int)
        # Asset type ON one-hot
        df['asset_type_ON'] = (df['type'].astype(str).str.contains('ON')).astype(int)
        # Day of week
        df['day_of_week'] = df['date'].dt.dayofweek
        # Price momentum 5
        df['price_momentum_5'] = grouped['close'].transform(lambda x: (x - x.shift(5)) / x.shift(5))
        # High breakout 20
        df['high_breakout_20'] = grouped['high'].transform(lambda x: (x == x.rolling(20).max()).astype(int))
        # Bollinger upper (20-day MA + 2*std)
        df['bollinger_upper'] = grouped['close'].transform(lambda x: x.rolling(20).mean() + 2 * x.rolling(20).std())
        # Stochastic 14
        def stochastic_14(series):
            low14 = series.rolling(14).min()
            high14 = series.rolling(14).max()
            return 100 * (series - low14) / (high14 - low14)
        df['stochastic_14'] = grouped['close'].transform(stochastic_14)
        # Drop rows with any NaNs in engineered features
        df = df.dropna(subset=[
            'daily_return', 'rolling_volatility_5', 'moving_avg_10', 'macd', 'rsi_14', 'volume_change', 'avg_volume_10',
            'best_buy_sell_spread', 'close_to_best_buy', 'market_type_NM', 'asset_type_ON', 'day_of_week',
            'price_momentum_5', 'high_breakout_20', 'bollinger_upper', 'stochastic_14'
        ])
        # Only keep the relevant columns
        keep_cols = ['date', 'ticker', 'company', 'daily_return', 'rolling_volatility_5', 'moving_avg_10', 'macd', 'rsi_14',
                     'volume_change', 'avg_volume_10', 'best_buy_sell_spread', 'close_to_best_buy', 'market_type_NM',
                     'asset_type_ON', 'day_of_week', 'price_momentum_5', 'high_breakout_20', 'bollinger_upper', 'stochastic_14']
        df = df[keep_cols]
        elapsed = time.time() - start_time
        logging.info(f"Transforming B3 hist quota.. (end) Elapsed: {elapsed:.2f} seconds")
        return df.reset_index(drop=True)
        