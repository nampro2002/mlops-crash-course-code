from pathlib import Path

import pandas as pd


def main() -> None:
    artifact_dir = Path(__file__).resolve().parent
    parquet_path = artifact_dir / "driver_stats.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing file: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    print(df)


if __name__ == "__main__":
    main()
