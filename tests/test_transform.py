"""Silver must flag a price that rounds to zero euros. The real H&M data holds prices around
0.000017 (0.0017 on the x100 scale), which price_eur rounds to 0.00 while price itself is above 0."""
import os
import sqlite3

import make_synthetic_data
from conftest import working_dir


def test_sub_cent_price_is_flagged_invalid_not_clean(tmp_path):
    import checks
    from ingest import ingest_bronze
    from transform import transform_silver

    make_synthetic_data.make(os.path.join(tmp_path, "data"), n_transactions=40_000)
    with working_dir(tmp_path):
        ingest_bronze()
        conn = sqlite3.connect("puma_dtc.db")
        conn.execute("UPDATE bronze_transactions SET price = 0.000017 WHERE rowid IN (3, 4)")
        conn.commit()
        conn.close()
        transform_silver()
        conn = sqlite3.connect("puma_dtc.db")
        flags = conn.execute("SELECT quality_flag, price_eur FROM silver_transactions WHERE price = 0.000017").fetchall()
        conn.close()
        report = checks.run_checks(("silver",), write=False)
    assert flags and all(flag == "invalid_price" and eur == 0 for flag, eur in flags)
    failed = report[report["status"] == "FAIL"]
    assert failed.empty, failed.to_string()
