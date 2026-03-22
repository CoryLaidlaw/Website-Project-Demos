#!/usr/bin/env python3
"""
Train binary NORMAL vs PNEUMONIA classifier on local Chest-XRay-Pneumonia layout.
Writes Keras model + metrics + demo sample thumbnails under repo paths.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
from pathlib import Path

import numpy as np
import tensorflow as tf
from PIL import Image
from sklearn.metrics import (
    accuracy_score,
    auc,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT_DEFAULT = REPO_ROOT / "data" / "Chest-XRay-Pneumonia"
ARTIFACTS = REPO_ROOT / "artifacts" / "cxr"
DEMO_MODEL_METRICS = REPO_ROOT / "model-metrics.json"
DEMO_SAMPLES_DIR = REPO_ROOT / "assets" / "samples"

IMG_SIZE = 224
BATCH_SIZE = 32
RNG_SEED = 42


def collect_paths_labels(data_root: Path) -> tuple[list[str], list[int]]:
    """NORMAL=0, PNEUMONIA=1. Supports NORMAL/PNEUMONIA or normal/pneumonia."""
    pairs: list[tuple[str, int]] = []
    variants = [
        (("NORMAL", "normal"), 0),
        (("PNEUMONIA", "pneumonia"), 1),
    ]
    for names, y in variants:
        folder = None
        for n in names:
            p = data_root / n
            if p.is_dir():
                folder = p
                break
        if folder is None:
            raise FileNotFoundError(
                f"Missing class folder under {data_root}: tried {names}"
            )
        for f in folder.iterdir():
            if f.suffix.lower() in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}:
                pairs.append((str(f.resolve()), y))
    paths = [p for p, _ in pairs]
    labels = [y for _, y in pairs]
    return paths, labels


def decode_and_preprocess(path: str, label: int):
    data = tf.io.read_file(path)
    img = tf.image.decode_image(data, channels=3, expand_animations=False)
    img.set_shape([None, None, 3])
    img = tf.image.resize(img, [IMG_SIZE, IMG_SIZE])
    img = tf.cast(img, tf.float32)
    img = preprocess_input(img)
    return img, tf.cast(label, tf.float32)


def augment(img, label):
    img = tf.image.random_flip_left_right(img)
    return img, label


def make_dataset(
    paths: list[str],
    labels: list[int],
    *,
    shuffle: bool,
    augment_train: bool,
) -> tf.data.Dataset:
    ds = tf.data.Dataset.from_tensor_slices((list(paths), list(labels)))
    if shuffle:
        ds = ds.shuffle(len(paths), seed=RNG_SEED, reshuffle_each_iteration=True)
    ds = ds.map(
        lambda p, y: decode_and_preprocess(p, y),
        num_parallel_calls=tf.data.AUTOTUNE,
    )
    if augment_train:
        ds = ds.map(augment, num_parallel_calls=tf.data.AUTOTUNE)
    ds = ds.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)
    return ds


def build_model() -> keras.Model:
    base = MobileNetV2(
        include_top=False,
        weights="imagenet",
        input_shape=(IMG_SIZE, IMG_SIZE, 3),
        pooling=None,
    )
    base.trainable = False
    inputs = keras.Input(shape=(IMG_SIZE, IMG_SIZE, 3))
    x = base(inputs, training=False)
    x = layers.GlobalAveragePooling2D()(x)
    x = layers.Dropout(0.25)(x)
    outputs = layers.Dense(1, activation="sigmoid")(x)
    model = keras.Model(inputs, outputs)
    return model


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DATA_ROOT_DEFAULT,
        help="Folder containing NORMAL and PNEUMONIA (or normal/pneumonia) image dirs",
    )
    parser.add_argument(
        "--epochs-head",
        type=int,
        default=4,
        help="Epochs with frozen MobileNet trunk",
    )
    parser.add_argument(
        "--epochs-finetune",
        type=int,
        default=10,
        help="Max epochs for fine-tuning (early stopping may stop sooner)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="1+1 epochs for smoke tests",
    )
    args = parser.parse_args()
    if args.quick:
        args.epochs_head = 1
        args.epochs_finetune = 1

    tf.keras.utils.set_random_seed(RNG_SEED)
    np.random.seed(RNG_SEED)
    random.seed(RNG_SEED)

    data_root: Path = args.data_root
    if not data_root.is_dir():
        raise SystemExit(f"Data root not found: {data_root}")

    paths, labels = collect_paths_labels(data_root)
    if len(paths) < 50:
        raise SystemExit(f"Too few images under {data_root}: {len(paths)}")

    X_train, X_temp, y_train, y_temp = train_test_split(
        paths,
        labels,
        test_size=0.3,
        stratify=labels,
        random_state=RNG_SEED,
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp,
        y_temp,
        test_size=0.5,
        stratify=y_temp,
        random_state=RNG_SEED,
    )

    train_ds = make_dataset(X_train, y_train, shuffle=True, augment_train=True)
    val_ds = make_dataset(X_val, y_val, shuffle=False, augment_train=False)
    test_ds = make_dataset(X_test, y_test, shuffle=False, augment_train=False)

    model = build_model()
    model.compile(
        optimizer=keras.optimizers.Adam(1e-3),
        loss=keras.losses.BinaryCrossentropy(),
        metrics=[
            keras.metrics.AUC(name="auc"),
            keras.metrics.BinaryAccuracy(name="accuracy"),
        ],
    )

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    cb_val = keras.callbacks.EarlyStopping(
        monitor="val_auc",
        mode="max",
        patience=3,
        restore_best_weights=True,
    )

    model.fit(
        train_ds,
        validation_data=val_ds,
        epochs=args.epochs_head,
        callbacks=[cb_val],
        verbose=1,
    )

    # Fine-tune last layers of MobileNet
    base = None
    for layer in model.layers:
        if isinstance(layer, keras.Model) and "mobilenet" in layer.name.lower():
            base = layer
            break
    if base is None:
        base = model.layers[1]
    assert isinstance(base, keras.Model)
    base.trainable = True
    fine_tune_at = len(base.layers) // 2
    for layer in base.layers[:fine_tune_at]:
        layer.trainable = False

    model.compile(
        optimizer=keras.optimizers.Adam(1e-5),
        loss=keras.losses.BinaryCrossentropy(),
        metrics=[
            keras.metrics.AUC(name="auc"),
            keras.metrics.BinaryAccuracy(name="accuracy"),
        ],
    )

    model.fit(
        train_ds,
        validation_data=val_ds,
        epochs=args.epochs_finetune,
        callbacks=[cb_val],
        verbose=1,
    )

    final_path = ARTIFACTS / "model.keras"
    model.save(final_path)
    print("Saved:", final_path)

    # Test-set metrics
    y_prob = model.predict(test_ds, verbose=0).ravel()
    y_true = np.array(y_test, dtype=int)
    y_pred = (y_prob >= 0.5).astype(int)

    acc = accuracy_score(y_true, y_pred)
    prec = precision_score(y_true, y_pred, zero_division=0)
    rec = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    roc_auc = auc(fpr, tpr)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()

    metrics_payload = {
        "task": "binary_cxr_pediatric",
        "classes": ["NORMAL", "PNEUMONIA"],
        "label_encoding": {"NORMAL": 0, "PNEUMONIA": 1},
        "positive_class": "PNEUMONIA",
        "input_size": [IMG_SIZE, IMG_SIZE],
        "channels": 3,
        "preprocessing": {
            "resize": [IMG_SIZE, IMG_SIZE],
            "mobilenet_v2_preprocess": "keras.applications.mobilenet_v2.preprocess_input (scale to [-1,1])",
        },
        "backbone": "MobileNetV2",
        "split": {
            "strategy": "stratified train/val/test",
            "fractions": {"train": 0.7, "val": 0.15, "test": 0.15},
            "random_seed": RNG_SEED,
            "counts": {
                "train": len(X_train),
                "val": len(X_val),
                "test": len(X_test),
            },
        },
        "test_metrics": {
            "accuracy": float(acc),
            "precision_pneumonia": float(prec),
            "recall_pneumonia": float(rec),
            "f1_pneumonia": float(f1),
            "roc_auc": float(roc_auc),
        },
        "confusion_matrix_test": {
            "labels_row_true_col_pred": ["NORMAL", "PNEUMONIA"],
            "matrix": [[int(tn), int(fp)], [int(fn), int(tp)]],
        },
        "limitations": [
            "Educational demo only; not validated for clinical use.",
            "Trained on a single public dataset; performance may not transfer to other scanners, ages, or sites.",
            "Binary task only; does not distinguish viral vs bacterial pneumonia or other pathology.",
        ],
    }

    DEMO_MODEL_METRICS.parent.mkdir(parents=True, exist_ok=True)
    with open(DEMO_MODEL_METRICS, "w", encoding="utf-8") as f:
        json.dump(metrics_payload, f, indent=2)
    shutil.copy2(DEMO_MODEL_METRICS, ARTIFACTS / "model-metrics.json")
    print("Wrote:", DEMO_MODEL_METRICS)

    # Demo samples: 3 NORMAL + 3 PNEUMONIA from test split
    DEMO_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    normal_idx = [i for i, p in enumerate(X_test) if y_test[i] == 0]
    pneu_idx = [i for i, p in enumerate(X_test) if y_test[i] == 1]
    rng = random.Random(RNG_SEED)
    pick_n = min(3, len(normal_idx))
    pick_p = min(3, len(pneu_idx))
    chosen = rng.sample(normal_idx, pick_n) + rng.sample(pneu_idx, pick_p)
    rng.shuffle(chosen)

    manifest_samples: list[dict] = []
    for i, idx in enumerate(chosen):
        src = Path(X_test[idx])
        true_y = y_test[idx]
        out_name = f"sample_{i + 1:02d}{src.suffix.lower() if src.suffix else '.jpg'}"
        out_path = DEMO_SAMPLES_DIR / out_name
        # Resize for small repo footprint (long edge 448)
        im = Image.open(src).convert("RGB")
        im.thumbnail((448, 448), Image.Resampling.LANCZOS)
        im.save(out_path, quality=82, optimize=True)
        note = (
            "Illustrative normal study; model may fail on atypical views or artifacts."
            if true_y == 0
            else "Illustrative pneumonia case; opacity patterns vary—do not infer diagnosis."
        )
        manifest_samples.append(
            {
                "id": f"s{i + 1}",
                "file": out_name,
                "trueLabel": "NORMAL" if true_y == 0 else "PNEUMONIA",
                "limitationsNote": note,
            }
        )

    manifest = {"samples": manifest_samples}
    with open(DEMO_SAMPLES_DIR / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print("Wrote demo samples:", DEMO_SAMPLES_DIR)

if __name__ == "__main__":
    os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
    main()
