# Changes: layer checks and agent hardening

## Where the files go

The zip mirrors your project folder, so unzipping it into `F:\Puma_Project` puts every file in the right place. `transform.py` is unchanged and not included.

| File | Status |
|---|---|
| `src\bronze\ingest.py` | changed: writes a `pipeline_audit` table (rows read vs rows written) |
| `src\gold\analytics.py` | changed: Gold reads only Silver rows flagged `clean` |
| `src\checks.py` | new: 32 checks across Bronze, Silver and Gold |
| `src\run_pipeline.py` | new: one command, checks between the stages, stops on the first failure |
| `src\agent\query_agent.py` | changed: read-only database, SQL guard, repair loop, number check |
| `src\dashboard\app.py` | changed: shows data checks, SQL and result, renders answers as text |
| `src\eval_agent.py` | new: 20 questions with reference queries |
| `startup.py` | changed: builds through `run_pipeline.run()` so the checks gate the build, and warns when an existing database has no passing check results |
| `tests\` | new: 105 tests |

Always run from `F:\Puma_Project`, as you do now. `puma_dtc.db` and `data\` are relative paths.

## Run

```
python src\run_pipeline.py              # Bronze, checks, Silver, checks, Gold, checks
python src\checks.py                    # only the checks (bronze | silver | gold | all)
python -m pytest tests                  # needs pytest, fastapi, httpx
python src\eval_agent.py                # needs GROQ_API_KEY, asks the agent 20 questions
python src\eval_agent.py --golden-only  # no API call
python src\dashboard\app.py             # the dashboard, http://localhost:8000
python startup.py                       # builds the database only if puma_dtc.db does not exist
```

`src\dashboard\app.py` needs Python 3.12 or newer. That was already true of your version (nested quotes inside f-strings).

## What the checks do

- **Bronze:** tables not empty; the realised sample fraction is 6%; Bronze row counts equal what `ingest.py` recorded; keys are unique; every transaction has an article.
- **Silver:** no rows lost or duplicated against Bronze; total price equals Bronze; every row has a division and channel; quality flags hold known values; clean rows have a positive price; dates are not in the future. WARN: share of unmapped categories, share of invalid prices, newest month incomplete.
- **Gold:** divisions match Silver and agree across the four tables; units sold in the last 3 months reconcile to Silver; sell-through and health scores in range; markdown cost between 0 and inventory value; labels known; no empty values.

Each run is written to `data_quality_report`; the dashboard shows it. FAIL stops the pipeline, WARN does not.

## What the agent does differently

- Opens the database read-only and installs a SQLite authorizer that allows SELECT on the four current Gold tables only. Writes and reads of other tables are refused by the database itself. The list is `GOLD_TABLES` at the top of `query_agent.py`: add a name there when you add a Gold table. Leftover `gold_` tables from older pipeline versions (the ones `clean_db.py` drops) are not shown to the model and cannot be read.
- Validates the SQL first (one statement, SELECT or WITH, Gold tables only). A rejected or failing query goes back to the model once with the error.
- Interrupts any query that runs longer than 5 seconds.
- If the model cannot be reached (no key, no network, rate limit), `ask()` returns an error message instead of crashing the request.
- Checks that every number in the answer appears in the query result (or in the question or the SQL). On a miss it asks for one rewrite, then returns the answer marked unverified.
- The answer prompt now tells the model to say "simulated" when it quotes stock, inventory value, weeks of stock or markdown cost.
- `ask()` returns a dict: `answer`, `sql`, `columns`, `rows`, `truncated`, `verified`, `ungrounded`, `error`. The dashboard shows the SQL, the result table and a verified / unverified badge. Model text is rendered with `textContent`, never as HTML (the old `innerHTML` line was an injection route).

## Behaviour changes you will notice

- **Gold has six divisions, not seven.** "Other" is exactly the rows flagged `unmapped_category`, and Gold now excludes flagged rows. Every Gold number shifts slightly.
- The dashboard's money KPI says "On simulated stock".

## Not verified

- **Real data.** Everything was tested on synthetic files with the same columns as yours. The thresholds in `checks.py` (5% unmapped, 0.5% invalid prices, sample tolerance) were set without seeing your data. Read the first real run and adjust them.
- **The live model.** Tests use a scripted stand-in for Groq. Nothing was sent to the API.
- **The evaluation pass rate.** `eval_agent.py` has not been run against the model. Quote a pass rate only after you run it.

## Still open

- Stock is simulated from hand-picked multipliers in `analytics.py`. Everything downstream of it (sell-through, weeks of stock, markdown risk, EUR exposure, health score) inherits that.
- The product-to-division mapping and `price * 100` in `transform.py` are assumptions, not data.
- If the real data ends mid-month, the last 3-month window is understated. The new "newest month is complete" WARN will tell you. I believe the H&M data ends on 22 Sep 2020, so expect it.

## Repo hygiene

Add `data/`, `*.db` and `.env` to `.gitignore`. Do not commit the H&M files: they are large, and Kaggle competition data normally carries usage rules, so check the competition's terms and link to the dataset in the README instead.
