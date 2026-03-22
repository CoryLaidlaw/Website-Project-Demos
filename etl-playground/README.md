# ETL playground

In-browser exploration of a multi-step **CSV cleaning pipeline**: parse, trim null tokens, dedupe, infer types, coerce dates/numbers, winsorize, impute, format money-like fields, and produce a **report** plus downloadable result. Uses small **sample CSVs** under [`etl-samples/`](etl-samples/) (messy sales, dirty customers, event log).

## Core files

| Path | Role |
|------|------|
| [`pipeline.js`](pipeline.js) | Step definitions and orchestration |
| [`app.js`](app.js) | File load, run pipeline, wire UI |
| [`report.js`](report.js) | Summary / table output |
| [`index.html`](index.html), [`styles.css`](styles.css) | Layout and styling |

## Stack

Plain **JavaScript** (no framework build). Intended as a teaching demo of data-prep logic, not a production ETL engine.
