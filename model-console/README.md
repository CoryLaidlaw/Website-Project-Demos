# Model console — phishing URL classifiers

Static “lab report” comparing **scikit-learn** pipelines trained on a public **URL vs label** dataset. The page reads precomputed JSON only (no training in the browser): a precision/recall scatter plot, per-model metrics, and methodology text.

## Models compared

Trained offline on the same stratified train/test split; see [`data/model-eval-results.json`](data/model-eval-results.json) for numbers.

1. **Logistic regression + char TF‑IDF** (`char_wb` n-grams)
2. **SGD (log loss) + char TF‑IDF**
3. **Complement Naive Bayes + binary char counts**

Features and evaluation details are in [`data/model-eval-meta.json`](data/model-eval-meta.json) (`methodology_note`, `metric_definitions`, limitations).

## Data

- **Dataset:** Phishing site URLs on Kaggle (binary `good` / `bad` labels). Links and license notes are in `model-eval-meta.json` (`source_urls`, `license_note`).
- **Raw CSV** is **not** committed here (large). To regenerate JSON, download the CSV into [`data/mlc-data/`](data/mlc-data/) (see [`data/mlc-data/README.md`](data/mlc-data/README.md)) and run the build script.

## Key files

| Path | Role |
|------|------|
| [`index.html`](index.html), [`styles.css`](styles.css) | Page and styling |
| [`app.js`](app.js) | Load JSON, Chart.js scatter, glossary, toggles |
| [`data/model-eval-results.json`](data/model-eval-results.json) | Per-model metrics and confusion counts |
| [`data/model-eval-meta.json`](data/model-eval-meta.json) | Title, methodology, dataset pointers |
| [`scripts/build_model_eval.py`](scripts/build_model_eval.py) | Regenerate JSON from a local CSV (`--csv` optional) |
| [`requirements-ml.txt`](requirements-ml.txt) | `scikit-learn`, `pandas`, etc. |

This demo is **educational**; it is not a production phishing filter.
