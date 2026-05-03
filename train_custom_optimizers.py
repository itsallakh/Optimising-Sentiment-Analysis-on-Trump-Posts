from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import expit
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score
from sklearn.model_selection import train_test_split


DEFAULT_INPUT = Path("factbase_truthsocial_texts_sentiment_labeled.csv")
DEFAULT_RESULTS_OUTPUT = Path("optimizer_results.csv")
DEFAULT_LOSS_OUTPUT = Path("optimizer_loss_history.csv")
DEFAULT_REPORT_OUTPUT = Path("optimizer_report.json")
DEFAULT_PREDICTIONS_OUTPUT = Path("optimizer_test_predictions.csv")
DEFAULT_PLOT_OUTPUT = Path("optimizer_loss_curves.png")

VALID_LABELS = {"positive", "negative"}
POSITIVE_LABEL = "positive"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare Batch GD, SGD, and Mini-batch GD for the same "
            "L2-regularized TF-IDF logistic regression classifier."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--results-output", type=Path, default=DEFAULT_RESULTS_OUTPUT)
    parser.add_argument("--loss-output", type=Path, default=DEFAULT_LOSS_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    parser.add_argument("--predictions-output", type=Path, default=DEFAULT_PREDICTIONS_OUTPUT)
    parser.add_argument("--plot-output", type=Path, default=DEFAULT_PLOT_OUTPUT)
    parser.add_argument("--text-column", default="text_for_nlp")
    parser.add_argument("--label-column", default="sentiment_label")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--min-df", type=int, default=2)
    parser.add_argument("--max-df", type=float, default=0.98)
    parser.add_argument("--max-features", type=int, default=5000)
    parser.add_argument("--epochs", type=int, default=300)
    parser.add_argument("--batch-lr", type=float, default=8.0)
    parser.add_argument("--sgd-lr", type=float, default=0.05)
    parser.add_argument("--minibatch-lr", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--l2", type=float, default=0.0001)
    parser.add_argument("--drop-duplicate-text", action="store_true")
    return parser.parse_args()


def build_dataset(args: argparse.Namespace) -> pd.DataFrame:
    df = pd.read_csv(args.input)
    required = {args.text_column, args.label_column}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    dataset = df.copy()
    dataset[args.text_column] = dataset[args.text_column].fillna("").astype(str)
    dataset[args.label_column] = dataset[args.label_column].fillna("").astype(str).str.strip().str.lower()
    dataset = dataset[dataset[args.text_column] != ""].copy()
    dataset = dataset[dataset[args.label_column].isin(VALID_LABELS)].copy()

    if args.drop_duplicate_text:
        dataset = dataset.drop_duplicates(subset=[args.text_column], keep="first").copy()

    if dataset.empty:
        raise ValueError("No labeled rows were available for training after filtering.")

    return dataset


def make_vectorizer(args: argparse.Namespace) -> TfidfVectorizer:
    return TfidfVectorizer(
        lowercase=False,
        strip_accents="unicode",
        ngram_range=(1, 2),
        min_df=args.min_df,
        max_df=args.max_df,
        max_features=args.max_features,
        sublinear_tf=True,
        norm="l2",
        token_pattern=r"(?u)\b(?=[a-z0-9']*[a-z])[a-z0-9']{2,}\b",
    )


def compute_loss(X, y: np.ndarray, weights: np.ndarray, bias: float, l2: float) -> float:
    logits = X @ weights + bias
    data_loss = np.mean(np.logaddexp(0.0, logits) - y * logits)
    reg_loss = 0.5 * l2 * float(weights @ weights)
    return float(data_loss + reg_loss)


def predict_proba(X, weights: np.ndarray, bias: float) -> np.ndarray:
    return expit(X @ weights + bias)


def predict_labels(X, weights: np.ndarray, bias: float) -> np.ndarray:
    return np.where(predict_proba(X, weights, bias) >= 0.5, "positive", "negative")


def train_batch_gd(X_train, y_train: np.ndarray, args: argparse.Namespace) -> tuple[np.ndarray, float, list[float]]:
    weights = np.zeros(X_train.shape[1], dtype=np.float64)
    bias = 0.0
    losses = []
    n_rows = X_train.shape[0]

    for _ in range(args.epochs):
        probabilities = predict_proba(X_train, weights, bias)
        errors = probabilities - y_train
        grad_weights = (X_train.T @ errors) / n_rows + args.l2 * weights
        grad_bias = float(np.mean(errors))
        weights -= args.batch_lr * np.asarray(grad_weights).ravel()
        bias -= args.batch_lr * grad_bias
        losses.append(compute_loss(X_train, y_train, weights, bias, args.l2))

    return weights, bias, losses


def train_sgd(X_train, y_train: np.ndarray, args: argparse.Namespace) -> tuple[np.ndarray, float, list[float]]:
    weights = np.zeros(X_train.shape[1], dtype=np.float64)
    bias = 0.0
    losses = []
    rng = np.random.default_rng(args.random_state)

    for _ in range(args.epochs):
        for row_idx in rng.permutation(X_train.shape[0]):
            row = X_train.getrow(row_idx)
            logit = float(row.dot(weights)[0] + bias)
            probability = float(expit(logit))
            error = probability - y_train[row_idx]
            weights *= 1.0 - args.sgd_lr * args.l2
            weights[row.indices] -= args.sgd_lr * error * row.data
            bias -= args.sgd_lr * error
        losses.append(compute_loss(X_train, y_train, weights, bias, args.l2))

    return weights, bias, losses


def train_minibatch_gd(X_train, y_train: np.ndarray, args: argparse.Namespace) -> tuple[np.ndarray, float, list[float]]:
    weights = np.zeros(X_train.shape[1], dtype=np.float64)
    bias = 0.0
    losses = []
    rng = np.random.default_rng(args.random_state)

    for _ in range(args.epochs):
        indices = rng.permutation(X_train.shape[0])
        for start in range(0, X_train.shape[0], args.batch_size):
            batch_idx = indices[start : start + args.batch_size]
            X_batch = X_train[batch_idx]
            y_batch = y_train[batch_idx]
            probabilities = predict_proba(X_batch, weights, bias)
            errors = probabilities - y_batch
            grad_weights = (X_batch.T @ errors) / len(batch_idx) + args.l2 * weights
            grad_bias = float(np.mean(errors))
            weights -= args.minibatch_lr * np.asarray(grad_weights).ravel()
            bias -= args.minibatch_lr * grad_bias
        losses.append(compute_loss(X_train, y_train, weights, bias, args.l2))

    return weights, bias, losses


def evaluate_optimizer(
    name: str,
    train_func,
    X_train,
    X_test,
    y_train: np.ndarray,
    y_test_labels: np.ndarray,
    args: argparse.Namespace,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    start = time.perf_counter()
    weights, bias, losses = train_func(X_train, y_train, args)
    runtime_seconds = time.perf_counter() - start

    y_pred = predict_labels(X_test, weights, bias)
    probabilities = predict_proba(X_test, weights, bias)
    result = {
        "optimizer": name,
        "epochs": args.epochs,
        "learning_rate": {
            "Batch GD": args.batch_lr,
            "SGD": args.sgd_lr,
            "Mini-batch GD": args.minibatch_lr,
        }[name],
        "batch_size": None if name != "Mini-batch GD" else args.batch_size,
        "l2": args.l2,
        "final_training_loss": losses[-1],
        "runtime_seconds": runtime_seconds,
        "test_accuracy": accuracy_score(y_test_labels, y_pred),
        "test_macro_f1": f1_score(y_test_labels, y_pred, average="macro"),
        "test_weighted_f1": f1_score(y_test_labels, y_pred, average="weighted"),
    }
    loss_rows = pd.DataFrame(
        {"optimizer": name, "epoch": np.arange(1, len(losses) + 1), "training_loss": losses}
    )
    prediction_rows = pd.DataFrame(
        {
            "optimizer": name,
            "actual_label": y_test_labels,
            "predicted_label": y_pred,
            "proba_positive": probabilities,
        }
    )
    return result, loss_rows, prediction_rows


def save_loss_plot(loss_history: pd.DataFrame, output: Path) -> bool:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    fig, ax = plt.subplots(figsize=(8, 5))
    for optimizer, rows in loss_history.groupby("optimizer"):
        ax.plot(rows["epoch"], rows["training_loss"], label=optimizer)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Training loss")
    ax.set_title("Optimizer convergence comparison")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)
    return True


def main() -> None:
    args = parse_args()
    dataset = build_dataset(args)
    y_labels = dataset[args.label_column].to_numpy()
    y_binary = (y_labels == POSITIVE_LABEL).astype(np.float64)

    X_train_text, X_test_text, y_train, y_test, y_train_labels, y_test_labels = train_test_split(
        dataset[args.text_column],
        y_binary,
        y_labels,
        test_size=args.test_size,
        random_state=args.random_state,
        stratify=y_labels,
    )

    vectorizer = make_vectorizer(args)
    X_train = vectorizer.fit_transform(X_train_text)
    X_test = vectorizer.transform(X_test_text)

    experiments = [
        ("Batch GD", train_batch_gd),
        ("SGD", train_sgd),
        ("Mini-batch GD", train_minibatch_gd),
    ]
    results = []
    loss_frames = []
    prediction_frames = []

    for name, train_func in experiments:
        result, loss_rows, prediction_rows = evaluate_optimizer(
            name, train_func, X_train, X_test, y_train, y_test_labels, args
        )
        results.append(result)
        loss_frames.append(loss_rows)
        prediction_frames.append(prediction_rows)
        print(
            f"{name}: loss={result['final_training_loss']:.4f}, "
            f"accuracy={result['test_accuracy']:.4f}, "
            f"macro_f1={result['test_macro_f1']:.4f}, "
            f"runtime={result['runtime_seconds']:.2f}s"
        )

    results_frame = pd.DataFrame(results)
    loss_history = pd.concat(loss_frames, ignore_index=True)
    predictions = pd.concat(prediction_frames, ignore_index=True)

    results_frame.to_csv(args.results_output, index=False)
    loss_history.to_csv(args.loss_output, index=False)
    predictions.to_csv(args.predictions_output, index=False)

    labels = ["negative", "positive"]
    report = {
        "input_file": str(args.input),
        "text_column": args.text_column,
        "label_column": args.label_column,
        "train_rows": int(X_train.shape[0]),
        "test_rows": int(X_test.shape[0]),
        "feature_count": int(X_train.shape[1]),
        "test_size": args.test_size,
        "random_state": args.random_state,
        "vectorizer": {
            "min_df": args.min_df,
            "max_df": args.max_df,
            "max_features": args.max_features,
            "ngram_range": [1, 2],
        },
        "objective": {
            "model": "binary logistic regression",
            "regularization": "L2",
            "l2": args.l2,
        },
        "results": results,
        "classification_reports": {},
        "confusion_matrices": {},
    }
    for optimizer, rows in predictions.groupby("optimizer"):
        report["classification_reports"][optimizer] = classification_report(
            rows["actual_label"], rows["predicted_label"], output_dict=True, zero_division=0
        )
        report["confusion_matrices"][optimizer] = {
            "labels": labels,
            "matrix": confusion_matrix(rows["actual_label"], rows["predicted_label"], labels=labels).tolist(),
        }
    args.report_output.write_text(json.dumps(report, indent=2))

    plot_saved = save_loss_plot(loss_history, args.plot_output)
    print(f"Saved optimizer results to: {args.results_output}")
    print(f"Saved loss history to: {args.loss_output}")
    print(f"Saved report to: {args.report_output}")
    print(f"Saved predictions to: {args.predictions_output}")
    if plot_saved:
        print(f"Saved loss plot to: {args.plot_output}")
    else:
        print("Skipped loss plot because matplotlib is not installed.")


if __name__ == "__main__":
    main()
