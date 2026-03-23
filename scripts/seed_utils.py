from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def current_snapshot_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def append_seed_rows(seed_path, snapshot_df, sort_columns):
    seed_path = Path(seed_path)
    seed_path.parent.mkdir(parents=True, exist_ok=True)

    normalized = snapshot_df.copy()
    normalized["snapshot_ts"] = normalized["snapshot_ts"].astype(str)

    existing_rows = 0

    if seed_path.exists():
        existing = pd.read_csv(seed_path)
        existing_rows = len(existing)

        all_columns = list(dict.fromkeys(existing.columns.tolist() + normalized.columns.tolist()))
        existing = existing.reindex(columns=all_columns)
        normalized = normalized.reindex(columns=all_columns)

        combined = pd.concat([existing, normalized], ignore_index=True)
        dedupe_columns = [column for column in all_columns if column != "snapshot_ts"]
        if dedupe_columns:
            combined = combined.drop_duplicates(subset=dedupe_columns, keep="first")
    else:
        combined = normalized

    order = [column for column in sort_columns if column in combined.columns]
    if "snapshot_ts" in combined.columns and "snapshot_ts" not in order:
        order.append("snapshot_ts")
    if order:
        combined = combined.sort_values(by=order, kind="stable", na_position="last")

    combined.to_csv(seed_path, index=False)
    return len(combined) - existing_rows
