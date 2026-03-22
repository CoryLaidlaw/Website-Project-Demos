#!/usr/bin/env python3
"""
Train baseline URL phishing classifiers and write static JSON for model-console/.

The raw Kaggle CSV is not vendored in this repo. Download it from the dataset page (see
model-eval-meta.json source_urls), place it at data/mlc-data/phishing_site_urls.csv, or pass --csv.

Columns: URL, Label (labels good/bad).
Writes: data/model-eval-results.json, data/model-eval-meta.json

Requires: pip install scikit-learn pandas (see requirements-ml.txt).

Usage:
  python3 scripts/build_model_eval.py
  python3 scripts/build_model_eval.py --csv /path/to/phishing_site_urls.csv
  python3 scripts/build_model_eval.py --max-rows 100000
  python3 scripts/build_model_eval.py --max-rows 0   # full dataset (slow, high memory)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import ComplementNB
from sklearn.pipeline import Pipeline
import sklearn

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "data" / "mlc-data" / "phishing_site_urls.csv"
OUT_RESULTS = ROOT / "data" / "model-eval-results.json"
OUT_META = ROOT / "data" / "model-eval-meta.json"

POS_LABEL = "bad"  # phishing
NEG_LABEL = "good"
RANDOM_STATE = 42
TEST_SIZE = 0.2


def load_xy(path: Path, max_rows: int | None, seed: int) -> tuple[pd.Series, pd.Series, dict[str, Any]]:
    df = pd.read_csv(path)
    if "URL" not in df.columns or "Label" not in df.columns:
        raise SystemExit("Expected columns URL and Label")
    df["Label"] = df["Label"].astype(str).str.strip().str.lower()
    df = df[df["Label"].isin((POS_LABEL, NEG_LABEL))].copy()
    n_total = len(df)
    y = (df["Label"] == POS_LABEL).astype(np.int8)
    X = df["URL"].astype(str)

    if max_rows and n_total > max_rows:
        X, _, y, _ = train_test_split(
            X,
            y,
            train_size=max_rows,
            stratify=y,
            random_state=seed,
        )
        X = X.reset_index(drop=True)
        y = y.reset_index(drop=True)

    stats = {
        "n_rows_total_file": n_total,
        "n_rows_used": int(len(y)),
        "prevalence_phishing": float(y.mean()),
        "label_counts": {"bad": int(y.sum()), "good": int(len(y) - y.sum())},
    }
    return X, y, stats


def make_models(seed: int) -> list[tuple[str, str, Pipeline]]:
    """Return (id, display_name, sklearn Pipeline)."""
    char_tfidf = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=4,
        max_df=0.98,
        max_features=50_000,
        sublinear_tf=True,
        dtype=np.float64,
    )
    char_count = CountVectorizer(
        analyzer="char",
        ngram_range=(3, 5),
        min_df=4,
        max_df=0.98,
        max_features=50_000,
        binary=True,
        dtype=np.int64,
    )

    m1 = Pipeline(
        [
            ("vec", char_tfidf),
            (
                "clf",
                LogisticRegression(
                    max_iter=2500,
                    class_weight="balanced",
                    solver="saga",
                    n_jobs=-1,
                    random_state=seed,
                ),
            ),
        ]
    )
    m2 = Pipeline(
        [
            ("vec", TfidfVectorizer(
                analyzer="char_wb",
                ngram_range=(3, 5),
                min_df=4,
                max_df=0.98,
                max_features=50_000,
                sublinear_tf=True,
                dtype=np.float64,
            )),
            (
                "clf",
                SGDClassifier(
                    loss="log_loss",
                    max_iter=2500,
                    class_weight="balanced",
                    random_state=seed,
                    tol=1e-3,
                ),
            ),
        ],
    )
    m3 = Pipeline(
        [
            ("vec", char_count),
            ("clf", ComplementNB()),
        ],
    )

    return [
        ("logreg_char_tfidf", "Logistic regression + char TF-IDF", m1),
        ("sgd_log_char_tfidf", "SGD (log loss) + char TF-IDF", m2),
        ("complement_nb_char_counts", "Complement Naive Bayes + char counts", m3),
    ]


def eval_binary(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_score: np.ndarray | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "accuracy": float(accuracy_score(y_true, y_pred)),
    }
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    out["confusion"] = {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}
    if y_score is not None and len(np.unique(y_true)) > 1:
        out["roc_auc"] = float(roc_auc_score(y_true, y_score))
        out["pr_auc"] = float(average_precision_score(y_true, y_score))
        try:
            out["brier"] = float(brier_score_loss(y_true, y_score))
        except ValueError:
            out["brier"] = None
    else:
        out["roc_auc"] = None
        out["pr_auc"] = None
        out["brier"] = None
    return out


def scores_for_model(pipe: Pipeline, X: Any) -> np.ndarray | None:
    if hasattr(pipe, "predict_proba"):
        return pipe.predict_proba(X)[:, 1]
    if hasattr(pipe, "decision_function"):
        return pipe.decision_function(X)
    return None


def guidance_for_models(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach plain-language when_to_pick / when_to_avoid from metric ordering."""
    by_prec = sorted(rows, key=lambda r: r["metrics_test"]["precision"], reverse=True)
    by_rec = sorted(rows, key=lambda r: r["metrics_test"]["recall"], reverse=True)
    by_f1 = sorted(rows, key=lambda r: r["metrics_test"]["f1"], reverse=True)
    best_p, best_r, best_f1 = by_prec[0]["id"], by_rec[0]["id"], by_f1[0]["id"]
    same_all = best_p == best_r == best_f1

    out = []
    for r in rows:
        mid = r["id"]
        lines_pick = []
        lines_avoid = []
        if same_all and mid == best_p:
            lines_pick.append(
                "Best precision, recall, and F1 among these three on this hold-out split—use as the default "
                "unless you have a different cost tradeoff than 0.5 threshold assumes."
            )
        else:
            if mid == best_p:
                lines_pick.append(
                    "You care most about precision: fewer legitimate URLs flagged as phishing "
                    "(lower false-positive rate at this threshold)."
                )
            if mid == best_r:
                lines_pick.append(
                    "You care most about recall: catching more phishing URLs even if that raises false alarms."
                )
            if mid == best_f1:
                lines_pick.append(
                    "You want a single balanced compromise between precision and recall (highest F1 here)."
                )
            if not lines_pick:
                lines_pick.append(
                    "Use this model if its precision/recall mix matches your cost tradeoff; compare the chart and table."
                )
        if mid != best_p and r["metrics_test"]["precision"] < by_prec[0]["metrics_test"]["precision"] - 1e-6:
            lines_avoid.append(
                "Avoid as the sole choice when false positives are very costly—another model had higher precision."
            )
        if mid != best_r and r["metrics_test"]["recall"] < by_rec[0]["metrics_test"]["recall"] - 1e-6:
            lines_avoid.append(
                "Avoid when missing phishing is unacceptable—another model achieved higher recall."
            )
        r = dict(r)
        r["when_to_pick"] = lines_pick
        r["when_to_avoid"] = lines_avoid or [
            "Not a substitute for browser blocklists, TLS inspection, or human review in production.",
        ]
        out.append(r)
    return out


def resolve_csv_path(p: Path) -> Path:
    path = p.expanduser()
    if not path.is_absolute():
        path = (ROOT / path).resolve()
    else:
        path = path.resolve()
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description="Build model-eval JSON for static site.")
    ap.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help="Path to phishing_site_urls.csv (URL, Label). Default: data/mlc-data/phishing_site_urls.csv under model-console.",
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=75_000,
        help="Stratified subsample size (default 75000). Use 0 for all rows.",
    )
    ap.add_argument("--seed", type=int, default=RANDOM_STATE)
    args = ap.parse_args()
    max_rows = None if args.max_rows == 0 else args.max_rows
    csv_path = resolve_csv_path(args.csv)

    if not csv_path.is_file():
        print(f"Missing {csv_path}", file=sys.stderr)
        sys.exit(1)

    X, y, load_stats = load_xy(csv_path, max_rows, args.seed)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        stratify=y,
        random_state=args.seed,
    )

    models = make_models(args.seed)
    model_rows: list[dict[str, Any]] = []

    for mid, display_name, pipe in models:
        pipe.fit(X_train, y_train)
        thr = 0.5
        scores = scores_for_model(pipe, X_test)
        if scores is not None:
            y_pred = (scores >= thr).astype(np.int8)
        else:
            y_pred = pipe.predict(X_test)

        metrics_test = eval_binary(y_test.to_numpy(), y_pred, scores)
        model_rows.append(
            {
                "id": mid,
                "display_name": display_name,
                "threshold": thr,
                "metrics_test": {k: v for k, v in metrics_test.items() if k != "confusion"},
                "confusion_test": metrics_test["confusion"],
            }
        )

    model_rows = guidance_for_models(model_rows)

    payload_core = {
        "schema_version": "1.0",
        "positive_class": {"value": 1, "raw_label": POS_LABEL, "role": "phishing"},
        "negative_class": {"value": 0, "raw_label": NEG_LABEL, "role": "legitimate"},
        "split": {
            "method": "stratified_train_test",
            "test_size": TEST_SIZE,
            "random_state": args.seed,
            "train_n": int(len(X_train)),
            "test_n": int(len(X_test)),
        },
        "load": load_stats,
        "models": model_rows,
        "tradeoff_summary": (
            "Phishing detection trades precision (blocking bad URLs without annoying users) "
            "against recall (catching abusive URLs). Metrics are on a single stratified hold-out split; "
            "they benchmark these pipelines on this snapshot, not production security."
        ),
    }

    h = hashlib.sha256(
        json.dumps(payload_core, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]
    results = dict(payload_core)
    results["results_id"] = f"sha256-{h}"

    OUT_RESULTS.parent.mkdir(parents=True, exist_ok=True)
    with OUT_RESULTS.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        f.write("\n")

    meta = {
        "title": "Phishing URL classifier comparison",
        "one_liner": "Offline-trained scikit-learn pipelines compared on a public URL dataset—static metrics and an explicit precision/recall tradeoff.",
        "dataset_name": "Phishing site URLs (Kaggle)",
        "dataset_path": (
            "Raw CSV not vendored in this repo; download from Kaggle (see source_urls) "
            "into data/mlc-data/phishing_site_urls.csv to regenerate metrics."
        ),
        "source_urls": [
            "https://www.kaggle.com/datasets/taruntiwarihp/phishing-site-urls",
        ],
        "license_note": (
            "Dataset licensing on Kaggle includes Open Database License (ODbL) and Database Contents License (DbCL) v1.0; "
            "verify the dataset page for attribution and any share-alike obligations. This site is not affiliated with Kaggle."
        ),
        "methodology_note": (
            "Labels are URL strings with binary flags good vs bad (phishing). "
            "Features are character n-gram bags: TF-IDF (char_wb) for linear models, binary char counts for Complement Naive Bayes. "
            "Preprocessing is fit inside each pipeline on the training split only. "
            "Evaluation uses a stratified 80/20 train/test split with a fixed random seed. "
            "Class imbalance is reported; linear models use class_weight='balanced' where applicable. "
            "No temporal split: URLs are not ordered by time—generalization to new campaigns is not established here."
        ),
        "limitations": [
            "Train/test split is random, not time-based—campaigns shift over time (distribution shift).",
            "Dataset may contain duplicated or near-duplicate URL patterns; we did not deduplicate aggressively.",
            "Metrics are point estimates on one split; confidence intervals are not shown.",
            "URL-only models are easy to evade; real systems combine blocklists, TLS, reputation, and user signals.",
            "This page is educational; it is not a phishing filter and must not be used as sole protection.",
        ],
        "metric_definitions": {
            "precision": "Of URLs predicted phishing, what fraction were truly phishing (equivalently, 1 − FP rate among predicted positives).",
            "recall": "Of truly phishing URLs, what fraction we caught (true positive rate).",
            "f1": "Harmonic mean of precision and recall.",
            "pr_auc": "Area under the precision–recall curve (average precision); informative under class imbalance.",
            "roc_auc": "Area under the ROC curve; can be optimistic when negatives dominate.",
            "brier": "Mean squared error of predicted probabilities vs labels (lower is better); only for models that output calibrated probabilities where applicable.",
        },
        "last_data_build": date.today().isoformat(),
        "results_file": "model-eval-results.json",
        "schema_version": "1.0",
        "build_tool": "scripts/build_model_eval.py",
        "sklearn_version": getattr(sklearn, "__version__", "unknown"),
    }

    with OUT_META.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Wrote {OUT_RESULTS} and {OUT_META} ({results['results_id']})")


if __name__ == "__main__":
    main()
