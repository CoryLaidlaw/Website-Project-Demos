# Pediatric CXR demo — dataset and reproduction

## Dataset (verified for portfolio use)

| Field | Value |
|-------|--------|
| **Name** | Pediatric Chest X-Ray Pneumonia — Balanced Dataset |
| **Source** | [Kaggle: Pediatric Chest X-Ray Pneumonia — Balanced Dataset](https://www.kaggle.com/datasets/yusufmurtaza01/chest-xray-pneumonia-balanced-dataset) |
| **Description (from listing)** | ~8,530 frontal chest X-rays, **NORMAL** vs **PNEUMONIA**, balanced, preprocessed for classification experiments. |
| **License (from listing)** | **CC0: Public Domain** |

Copy the license and citation from the dataset page if it changes; do not claim clinical validity.

## Local layout (not committed)

Place extracted images under:

- `cxr-demo/data/Chest-XRay-Pneumonia/NORMAL/` — class NORMAL  
- `cxr-demo/data/Chest-XRay-Pneumonia/PNEUMONIA/` — class PNEUMONIA  

(If your unpack uses lowercase folder names, adjust paths in `scripts/train_cxr.py` or rename folders.)

## Train and export (offline)

Python 3.10+ recommended. From the **`cxr-demo/`** directory:

```bash
cd /path/to/Website-Project-Demos/cxr-demo
python3 -m venv .venv-cxr
source .venv-cxr/bin/activate   # Windows: .venv-cxr\Scripts\activate
pip install -r requirements-cxr.txt
python scripts/train_cxr.py
python scripts/export_cxr_tfjs.py
```

- Training writes `artifacts/cxr/` (Keras model, metrics JSON, sample list).  
- Export writes a **TensorFlow.js GraphModel** (`format: graph-model` in `model/model.json`) to **`model/`** via SavedModel → `tfjs_graph_model`. The page uses `tf.loadGraphModel` so the browser does not hit Keras 3 nested-layer issues with `loadLayersModel`.  
- Demo thumbnails are refreshed under **`assets/samples/`** when you run training (or copy manually).

**Export troubleshooting:** If `import tensorflowjs` fails with a Protobuf / `tensorflow_decision_forests` version error, upgrade Protobuf in the same venv (for example `pip install 'protobuf>=6.31'`), run the export, then consider restoring a TensorFlow-compatible protobuf if you need to retrain (`tensorflow` 2.18 pins `protobuf<6`).

## Static preview

From the **repository root** (parent of `cxr-demo/`):

```bash
cd /path/to/Website-Project-Demos
python3 -m http.server 8080
```

Open `http://localhost:8080/cxr-demo/`.

## Ethics

Educational portfolio only. Not for diagnosis or treatment decisions. See disclaimers on the demo page.
