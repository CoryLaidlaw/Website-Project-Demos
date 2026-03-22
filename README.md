# Website-Project-Demos

This repository is a **read-only mirror** of the code behind interactive demos on my portfolio site. It exists so visitors can inspect the implementation and offline training scripts. The **canonical demos** run on the website; this repo is not the primary deployment target.

## What’s in here

| Folder | Description |
|--------|-------------|
| [cxr-demo/](cxr-demo/) | Pediatric chest X-ray **NORMAL vs PNEUMONIA** browser demo (TensorFlow.js). MobileNetV2-trained model, curated sample images. |
| [model-console/](model-console/) | **Scikit-learn** URL classifier comparison: precision/recall tradeoff from precomputed evaluation JSON (phishing vs legitimate labels). |
| [dashboard/](dashboard/) | US electricity **generation mix over time** from EIA-shaped data; static JSON and charts. |
| [etl-playground/](etl-playground/) | In-browser **ETL-style** pipeline over small sample CSVs (parse, clean, infer types, report). |

## Tech at a glance

- **CXR demo:** TensorFlow/Keras training, TensorFlow.js **GraphModel** inference, vanilla JS.
- **Model console:** `scikit-learn` (logistic regression + char TF‑IDF, SGD + char TF‑IDF, Complement Naive Bayes + char counts), metrics baked into JSON; Chart.js for the scatter plot.
- **Dashboard:** Python aggregator for EIA-style annual generation data; static JSON consumed by the front end.
- **ETL playground:** Vanilla JS pipeline modules, no build step.

## License

See [LICENSE](LICENSE) (MIT).
