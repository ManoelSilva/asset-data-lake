import pandas as pd


class B3HistFileParser(object):
    def __init__(self, file_path: str):
        self._file_path = file_path


    def parse_b3_hist_quota(self) -> pd.DataFrame:
        colspecs = [
            (2, 10),  # date
            (10, 12),  # bdi_code
            (12, 24),  # ticker
            (27, 39),  # company
            (39, 49),  # type_stock
            (49, 52),  # market
            (52, 56),  # currency
            (56, 69),  # open
            (69, 82),  # high
            (82, 95),  # low
            (95, 108),  # avg
            (108, 121),  # close
            (121, 134),  # best_buy
            (134, 147),  # best_sell
            (147, 152),  # trades
            (152, 170),  # volume
            (170, 188),  # turnover
            (230, 242)  # ISIN
        ]

        names = [
            "date", "bdi_code", "ticker", "company", "type", "market", "currency",
            "open", "high", "low", "avg", "close", "best_buy", "best_sell",
            "trades", "volume", "turnover", "isin"
        ]

        df = pd.read_fwf(self._file_path, colspecs=colspecs, names=names, dtype=str)

        # Filter only trading records (exclude 00 header and 99 trailer)
        df = df[df["date"].str.fullmatch(r"\d{8}")]

        # Convert types
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")

        for col in ["open", "high", "low", "avg", "close", "best_buy", "best_sell", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100

        df["trades"] = pd.to_numeric(df["trades"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

        return df.reset_index(drop=True)
