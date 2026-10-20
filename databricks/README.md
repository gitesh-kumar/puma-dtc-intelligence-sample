# The pipeline on Databricks

Silver and Gold of the DTC pipeline as PySpark and Delta tables on Databricks Free Edition, plus a
notebook that compares every Gold number with your local pandas run.

What a green run shows: two implementations of the same rules agree on your data. It does not show
that either one is right, and the stock is still simulated.

## Files

| File | Where it runs | What it does |
|---|---|---|
| `export_bronze.py` | your machine | copies the Bronze tables to Parquet and writes `local_reference.json` with your local numbers |
| `01_pipeline.py` | Databricks | Bronze, Silver, Gold as Delta tables, with the 32 checks between the layers |
| `02_parity.py` | Databricks | compares the Databricks tables with `local_reference.json` |
| `dtc_spark.py` | Databricks | the PySpark port of `transform.py` and `analytics.py`, and the bridge that runs `checks.py` unchanged on Spark |
| `dtc_reference.py` | both | the reference file and the comparison (pandas only) |

`checks.py` is not copied. The notebooks import the one in `src\`.

## Steps

**1. Local run, clean.** From the project folder:

    python src\run_pipeline.py --skip-ingest

It must end with `Pipeline finished, all checks passed.` WARN lines are fine, FAIL stops it.

**2. Export.**

    pip install pyarrow
    python databricks\export_bronze.py

This writes four files into `databricks_export\`. Add that folder to `.gitignore`, it holds the data.
The script refuses to run when the local report has a FAIL or a table is missing.

**3. Get the code into Databricks.** Sign up for Databricks Free Edition. Two ways:

- Git folder (preferred): push the project to GitHub, then in the workspace choose Create, Git folder, paste the
  repository URL. The notebooks then sit in `databricks/` and find `../src/checks.py` by themselves.
- Manual: create a workspace folder, import `01_pipeline.py` and `02_parity.py` into it (they become notebooks),
  and upload `dtc_spark.py`, `dtc_reference.py` and `checks.py` into the same folder as files.

**4. Run `01_pipeline`.** Attach it to serverless compute. Run the first two cells, they create the schema
`workspace.puma_dtc` and the volume `landing` (if your default catalog is not called `workspace`, change
`CATALOG` in the first cell). Then upload the four files: Catalog, `workspace`, `puma_dtc`, `landing`,
Upload to this volume. Run the rest. It prints the check summary of every layer, like `run_pipeline.py`.

**5. Run `02_parity`.** The last cell must print `PARITY PASS`. Take a screenshot of the cell output and of the
`parity_report` table.

## If it fails

| Message | Cause |
|---|---|
| `Upload these files to /Volumes/...` | the four files are not in the volume yet |
| `ModuleNotFoundError: dtc_spark` | the notebook is not in the same folder as `dtc_spark.py` |
| `checks.py not found` | `checks.py` is neither next to `dtc_spark.py` nor in `../src` |
| `PipelineFailed: bronze` | an upload is cut short, the row counts no longer match the audit |
| `PARITY FAIL` | open `parity_report`, filter `status = FAIL`, the `detail` column names the rows that differ |
| compute stops, quota | Free Edition shuts compute down for the rest of the day when the quota is used up, run again tomorrow |

A money column may differ by up to one cent, pandas and Spark add floating point numbers in a different order.
Anything bigger, and every count, rate and label, must be identical.

## Keeping it true

The port is a second copy of the logic. When `transform.py`, `analytics.py` or `checks.py` change, change the
port as well, export again and run both notebooks again. `tests\test_spark_parity.py` does this on synthetic data
(needs Java 17 or newer and `pip install pyspark pyarrow`, it skips itself otherwise). `DTC_SPARK_MODE=connect`
runs it through Spark Connect, the way serverless talks to Spark.

## What was checked, and what was not

Checked on synthetic data, with local Spark: all four Gold tables, the Silver group totals and the 32 checks
agree with the pandas pipeline. ANSI mode on and off, two session time zones, Spark Connect. A second data set with
missing and tiny prices, an unknown article, an unknown channel, boundary ages, a division without prior sales
and a December month end. Seven deliberate changes to the port were all caught by the comparison.

Not checked: Databricks itself (the volume, the upload, serverless limits, importing the notebooks), and the
real H&M data. Both are what your run proves.

## What you may say after a green run

Only what you did: Silver and Gold of your own DTC pipeline, ported to PySpark, run on Databricks Free Edition
with serverless compute, and compared with the local pandas run, with the number of comparisons that agreed.
Not that you work with Databricks in production, not Azure Databricks, not Power BI, not Unity Catalog beyond
the schema and volume the notebook creates. Free Edition is for non-commercial use, so this stays a portfolio
project.

## Limits

- Bronze is a copy of your local Bronze tables. Free Edition reaches only trusted domains, so the Kaggle
  download and the 6% sampling stay local.
- The simulated stock multipliers are copied from `analytics.py`. Sell-through and weeks of stock are set by
  them, not by the sales.
- Silver and Gold are rebuilt in full on every run. There is no incremental load.
