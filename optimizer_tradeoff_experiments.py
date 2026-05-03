from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split

from train_custom_optimizers import (
    POSITIVE_LABEL,
    build_dataset,
    predict_labels,
    predict_proba,
    train_batch_gd,
    train_minibatch_gd,
    train_sgd,
)


DEFAULT_INPUT = Path("factbase_truthsocial_texts_sentiment_labeled.csv")
DEFAULT_RESULTS_OUTPUT = Path("optimizer_tradeoff_results.csv")
DEFAULT_LOSS_OUTPUT = Path("optimizer_tradeoff_loss_history.csv")
DEFAULT_STABILITY_OUTPUT = Path("optimizer_stability_summary.csv")
DEFAULT_REPORT_OUTPUT = Path("optimizer_tradeoff_report.json")


@dataclass(frozen=True)
class ExperimentConfig:
    scenario: str
    optimizer: str
    learning_rate: float
    batch_size: int | None
    l2: float
    seed: int
    epochs: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run optimization tradeoff experiments for Batch GD, SGD, and Mini-batch GD "
            "on the same TF-IDF + L2 logistic regression objective."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--results-output", type=Path, default=DEFAULT_RESULTS_OUTPUT)
    parser.add_argument("--loss-output", type=Path, default=DEFAULT_LOSS_OUTPUT)
    parser.add_argument("--stability-output", type=Path, default=DEFAULT_STABILITY_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    parser.add_argument("--text-column", default="text_for_nlp")
    parser.add_argument("--label-column", default="sentiment_label")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--min-df", type=int, default=2)
    parser.add_argument("--max-df", type=float, default=0.98)
    parser.add_argument("--max-features", type=int, default=5000)
    parser.add_argument("--epochs", type=int, default=80)
    parser.add_argument("--convergence-tol", type=float, default=1e-4)
    parser.add_argument("--convergence-patience", type=int, default=5)
    parser.add_argument("--drop-duplicate-text", action="store_true")
    return parser.parse_args()


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


def train_for_config(config: ExperimentConfig, X_train, y_train: np.ndarray) -> tuple[np.ndarray, float, list[float]]:
    lr_kwargs = {
        "Batch GD": {"batch_lr": config.learning_rate, "sgd_lr": 0.05, "minibatch_lr": 1.0},
        "SGD": {"batch_lr": 8.0, "sgd_lr": config.learning_rate, "minibatch_lr": 1.0},
        "Mini-batch GD": {"batch_lr": 8.0, "sgd_lr": 0.05, "minibatch_lr": config.learning_rate},
    }[config.optimizer]
    train_args = SimpleNamespace(
        epochs=config.epochs,
        batch_size=config.batch_size or 64,
        l2=config.l2,
        random_state=config.seed,
        **lr_kwargs,
    )
    train_func = {
        "Batch GD": train_batch_gd,
        "SGD": train_sgd,
        "Mini-batch GD": train_minibatch_gd,
    }[config.optimizer]
    return train_func(X_train, y_train, train_args)


def convergence_epoch(losses: list[float], tol: float, patience: int) -> int | None:
    if len(losses) <= patience:
        return None

    improvements = np.array(losses[:-1]) - np.array(losses[1:])
    for idx in range(0, len(improvements) - patience + 1):
        window = improvements[idx : idx + patience]
        if np.all(np.abs(window) < tol):
            return idx + patience + 1
    return None


def loss_roughness(losses: list[float]) -> float:
    if len(losses) < 3:
        return 0.0
    diffs = np.diff(losses)
    return float(np.std(diffs))


def epoch_to_loss_reduction_fraction(losses: list[float], fraction: float = 0.9) -> int | None:
    if len(losses) < 2:
        return None

    initial_loss = losses[0]
    final_loss = losses[-1]
    total_reduction = initial_loss - final_loss
    if total_reduction <= 0:
        return None

    target_loss = initial_loss - fraction * total_reduction
    for epoch, loss in enumerate(losses, start=1):
        if loss <= target_loss:
            return epoch
    return None


def run_one_experiment(
    config: ExperimentConfig,
    X_train,
    X_test,
    y_train: np.ndarray,
    y_test_labels: np.ndarray,
    tol: float,
    patience: int,
) -> tuple[dict, pd.DataFrame]:
    start = time.perf_counter()
    weights, bias, losses = train_for_config(config, X_train, y_train)
    runtime_seconds = time.perf_counter() - start

    y_pred = predict_labels(X_test, weights, bias)
    probabilities = predict_proba(X_test, weights, bias)
    result = {
        **asdict(config),
        "final_training_loss": losses[-1],
        "initial_training_loss": losses[0],
        "loss_reduction": losses[0] - losses[-1],
        "loss_roughness": loss_roughness(losses),
        "convergence_epoch": convergence_epoch(losses, tol, patience),
        "best_loss_epoch": int(np.argmin(losses) + 1),
        "epoch_to_90pct_loss_reduction": epoch_to_loss_reduction_fraction(losses, fraction=0.9),
        "runtime_seconds": runtime_seconds,
        "test_accuracy": accuracy_score(y_test_labels, y_pred),
        "test_macro_f1": f1_score(y_test_labels, y_pred, average="macro"),
        "test_weighted_f1": f1_score(y_test_labels, y_pred, average="weighted"),
        "positive_prediction_rate": float(np.mean(probabilities >= 0.5)),
    }
    loss_rows = pd.DataFrame(
        {
            **asdict(config),
            "epoch": np.arange(1, len(losses) + 1),
            "training_loss": losses,
        }
    )
    return result, loss_rows


def build_experiment_grid(epochs: int) -> list[ExperimentConfig]:
    configs: list[ExperimentConfig] = []
    base_seed = 42
    base_l2 = 0.0001

    learning_rates = {
        "Batch GD": [1.0, 4.0, 8.0, 12.0],
        "SGD": [0.005, 0.02, 0.05, 0.1],
        "Mini-batch GD": [0.1, 0.5, 1.0, 2.0],
    }
    for optimizer, rates in learning_rates.items():
        for learning_rate in rates:
            configs.append(
                ExperimentConfig(
                    scenario="learning_rate_sensitivity",
                    optimizer=optimizer,
                    learning_rate=learning_rate,
                    batch_size=64 if optimizer == "Mini-batch GD" else None,
                    l2=base_l2,
                    seed=base_seed,
                    epochs=epochs,
                )
            )

    for batch_size in [8, 16, 32, 64, 128]:
        configs.append(
            ExperimentConfig(
                scenario="mini_batch_size",
                optimizer="Mini-batch GD",
                learning_rate=1.0,
                batch_size=batch_size,
                l2=base_l2,
                seed=base_seed,
                epochs=epochs,
            )
        )

    for optimizer in ["SGD", "Mini-batch GD"]:
        for seed in [1, 7, 42, 99, 123]:
            configs.append(
                ExperimentConfig(
                    scenario="stability_across_seeds",
                    optimizer=optimizer,
                    learning_rate=0.05 if optimizer == "SGD" else 1.0,
                    batch_size=64 if optimizer == "Mini-batch GD" else None,
                    l2=base_l2,
                    seed=seed,
                    epochs=epochs,
                )
            )

    for l2 in [0.00001, 0.0001, 0.001, 0.01]:
        for optimizer in ["Batch GD", "SGD", "Mini-batch GD"]:
            configs.append(
                ExperimentConfig(
                    scenario="l2_interaction",
                    optimizer=optimizer,
                    learning_rate={"Batch GD": 8.0, "SGD": 0.05, "Mini-batch GD": 1.0}[optimizer],
                    batch_size=64 if optimizer == "Mini-batch GD" else None,
                    l2=l2,
                    seed=base_seed,
                    epochs=epochs,
                )
            )

    return configs


def make_stability_summary(results: pd.DataFrame) -> pd.DataFrame:
    stability = results[results["scenario"] == "stability_across_seeds"].copy()
    if stability.empty:
        return pd.DataFrame()

    grouped = stability.groupby(["scenario", "optimizer", "learning_rate", "batch_size", "l2"], dropna=False)
    return grouped.agg(
        runs=("seed", "count"),
        mean_final_loss=("final_training_loss", "mean"),
        std_final_loss=("final_training_loss", "std"),
        mean_runtime_seconds=("runtime_seconds", "mean"),
        std_runtime_seconds=("runtime_seconds", "std"),
        mean_accuracy=("test_accuracy", "mean"),
        std_accuracy=("test_accuracy", "std"),
        mean_macro_f1=("test_macro_f1", "mean"),
        std_macro_f1=("test_macro_f1", "std"),
        mean_weighted_f1=("test_weighted_f1", "mean"),
        std_weighted_f1=("test_weighted_f1", "std"),
    ).reset_index()


def main() -> None:
    args = parse_args()
    dataset = build_dataset(args)
    y_labels = dataset[args.label_column].to_numpy()
    y_binary = (y_labels == POSITIVE_LABEL).astype(np.float64)

    X_train_text, X_test_text, y_train, _, _, y_test_labels = train_test_split(
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

    results = []
    loss_frames = []
    configs = build_experiment_grid(args.epochs)

    for idx, config in enumerate(configs, start=1):
        result, loss_rows = run_one_experiment(
            config,
            X_train,
            X_test,
            y_train,
            y_test_labels,
            tol=args.convergence_tol,
            patience=args.convergence_patience,
        )
        results.append(result)
        loss_frames.append(loss_rows)
        print(
            f"[{idx}/{len(configs)}] {config.scenario} | {config.optimizer} "
            f"lr={config.learning_rate} batch={config.batch_size} l2={config.l2} seed={config.seed}: "
            f"loss={result['final_training_loss']:.4f}, "
            f"macro_f1={result['test_macro_f1']:.4f}, "
            f"runtime={result['runtime_seconds']:.2f}s"
        )

    results_frame = pd.DataFrame(results)
    loss_history = pd.concat(loss_frames, ignore_index=True)
    stability_summary = make_stability_summary(results_frame)

    results_frame.to_csv(args.results_output, index=False)
    loss_history.to_csv(args.loss_output, index=False)
    stability_summary.to_csv(args.stability_output, index=False)

    report = {
        "input_file": str(args.input),
        "text_column": args.text_column,
        "label_column": args.label_column,
        "train_rows": int(X_train.shape[0]),
        "test_rows": int(X_test.shape[0]),
        "feature_count": int(X_train.shape[1]),
        "epochs_per_run": args.epochs,
        "convergence_tol": args.convergence_tol,
        "convergence_patience": args.convergence_patience,
        "scenarios": sorted(results_frame["scenario"].unique().tolist()),
        "output_files": {
            "results": str(args.results_output),
            "loss_history": str(args.loss_output),
            "stability_summary": str(args.stability_output),
        },
    }
    args.report_output.write_text(json.dumps(report, indent=2))

    print(f"Saved tradeoff results to: {args.results_output}")
    print(f"Saved tradeoff loss history to: {args.loss_output}")
    print(f"Saved stability summary to: {args.stability_output}")
    print(f"Saved tradeoff report to: {args.report_output}")


if __name__ == "__main__":
    main()
