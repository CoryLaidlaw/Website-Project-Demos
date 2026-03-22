# Pediatric chest X-ray demo (NORMAL vs PNEUMONIA)

Educational browser demo: run a small **TensorFlow.js** model on curated frontal chest X-ray samples and inspect **P(PNEUMONIA)**. **Not for diagnosis or clinical use.**

## Model and stack

- **Architecture:** **MobileNetV2** backbone (ImageNet weights) with a binary head; training uses Keras/TensorFlow in Python.
- **Inference:** Exported **TensorFlow.js GraphModel** ([`model/model.json`](model/model.json)); the page loads it with `tf.loadGraphModel` (avoids Keras 3 nested-layer issues with `loadLayersModel`).
- **Preprocessing:** Resize to 224×224, MobileNet v2 `preprocess_input` (scale to [-1, 1]), matching [`app.js`](app.js).

## Key files

| Path | Role |
|------|------|
| [`index.html`](index.html), [`demo.css`](demo.css) | Page layout and styling |
| [`app.js`](app.js) | Load model, metrics, sample manifest; run inference and UI |
| [`model/`](model/) | TensorFlow.js graph model shards |
| [`model-metrics.json`](model-metrics.json) | Test-set metrics from the offline training run |
| [`assets/samples/`](assets/samples/) | Thumbnails + [`manifest.json`](assets/samples/manifest.json) (true labels and limitation notes) |

## Training data

Images for **training** are **not** stored in this repo (too large). Dataset source, license, and local folder layout for reproduction are documented in **[`docs/cxr-demo.md`](docs/cxr-demo.md)** (Kaggle pediatric chest X-ray dataset, CC0 per that page).

## Scripts

- [`scripts/train_cxr.py`](scripts/train_cxr.py) — train on local `data/Chest-XRay-Pneumonia/`, write `artifacts/cxr/`, refresh `model-metrics.json` and `assets/samples/`.
- [`scripts/export_cxr_tfjs.py`](scripts/export_cxr_tfjs.py) — export Keras checkpoint to `model/`.
- [`requirements-cxr.txt`](requirements-cxr.txt) — Python dependencies.

Run commands and static preview URLs are in [`docs/cxr-demo.md`](docs/cxr-demo.md).
