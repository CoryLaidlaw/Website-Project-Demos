#!/usr/bin/env python3
"""
Export artifacts/cxr/model.keras to TensorFlow.js GraphModel under cxr-demo/model/.

Uses SavedModel -> tfjs_graph_model (not layers) so the browser avoids Keras 3 nested-model
InputLayer issues with tf.loadLayersModel.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from tensorflow import keras

REPO_ROOT = Path(__file__).resolve().parents[1]
KERAS_MODEL = REPO_ROOT / "artifacts" / "cxr" / "model.keras"
EXPORT_DIR = REPO_ROOT / "model"


def main() -> None:
    if not KERAS_MODEL.is_file():
        raise SystemExit(
            f"Missing trained model: {KERAS_MODEL}\nRun: python scripts/train_cxr.py"
        )
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    for p in EXPORT_DIR.iterdir():
        if p.is_file():
            p.unlink()

    model = keras.models.load_model(KERAS_MODEL)

    with tempfile.TemporaryDirectory(prefix="cxr_saved_") as tmp:
        saved = Path(tmp) / "saved_model"
        # Keras 3: export() writes a TensorFlow SavedModel directory.
        model.export(str(saved))

        # Same interpreter as training env
        converter = Path(sys.executable).parent / "tensorflowjs_converter"
        if not converter.is_file():
            converter = shutil.which("tensorflowjs_converter")
        if not converter:
            raise SystemExit("tensorflowjs_converter not found in PATH or venv")

        cmd = [
            str(converter),
            "--input_format=tf_saved_model",
            "--output_format=tfjs_graph_model",
            str(saved),
            str(EXPORT_DIR),
        ]
        subprocess.run(cmd, check=True)

    print("Exported TensorFlow.js GraphModel to:", EXPORT_DIR)


if __name__ == "__main__":
    main()
