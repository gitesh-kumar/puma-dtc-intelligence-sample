# Deploy snapshot

`puma_dtc_demo.sqlite` is what the deployed dashboard runs on. It holds the four Gold tables, the data check
report and the period of the sales data, a few kilobytes, no raw H&M data.

The full database is not in git. It is built from 3.5 GB of Kaggle data, and a free host cannot build it on
every start. `startup.py --serve` copies the snapshot to `puma_dtc.db` when there is no database and no source
data, then starts the dashboard.

Refresh it after a pipeline run you want to publish:

    python src\run_pipeline.py --skip-ingest
    python src\make_demo_db.py
    git add deploy\puma_dtc_demo.sqlite
    git commit -m "Refresh demo snapshot"
    git push

`make_demo_db.py` refuses to write a snapshot when a check failed or a layer was never checked.
The demo shows the numbers of your last local run. The stock in them is simulated.
