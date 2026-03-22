# Phishing URL dataset (local only)

This folder is for the Kaggle **Phishing Site URLs** CSV used when re-running the offline evaluation script.

1. Download the dataset from [Kaggle: Phishing site URLs](https://www.kaggle.com/datasets/taruntiwarihp/phishing-site-urls) (see also `source_urls` in `../model-eval-meta.json`).
2. Place the file here as **`phishing_site_urls.csv`** with columns `URL` and `Label` (`good` / `bad`).

The committed repo ships **precomputed** metrics in `../model-eval-results.json`; you only need this file to regenerate those JSON files.
