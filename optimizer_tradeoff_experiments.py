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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split

from train_custom_optimizers import (
    POSITIVE_LABEL,
    build_dataset,
    compute_loss,
    predict_labels,
    predict_proba,
    train_batch_gd,
    train_minibatch_gd,
    train_sgd,
)


DEFAULT_INPUT = Path("factbase_truthsocial_texts_sentiment_labeled.csv")
DEFAULT_VALIDATION_RESULTS_OUTPUT = Path("validation_optimizer_results.csv")
DEFAULT_FINAL_TEST_RESULTS_OUTPUT = Path("final_test_optimizer_results.csv")
DEFAULT_SELECTED_CONFIGS_OUTPUT = Path("selected_optimizer_configs.csv")
DEFAULT_LOSS_OUTPUT = Path("validation_optimizer_loss_history.csv")
DEFAULT_STABILITY_OUTPUT = Path("validation_optimizer_stability_summary.csv")
DEFAULT_SEED_STABILITY_OUTPUT = Path("optimizer_seed_stability_summary.csv")
DEFAULT_SEED_STABILITY_PLOT_OUTPUT = Path("results/figures/seed_stability_boxplot.png")
DEFAULT_REPORT_OUTPUT = Path("validation_optimizer_report.json")
CUSTOM_OPTIMIZERS = {"Batch GD", "SGD", "Mini-batch GD"}


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
    parser.add_argument("--validation-results-output", type=Path, default=DEFAULT_VALIDATION_RESULTS_OUTPUT)
    parser.add_argument("--final-test-results-output", type=Path, default=DEFAULT_FINAL_TEST_RESULTS_OUTPUT)
    parser.add_argument("--selected-configs-output", type=Path, default=DEFAULT_SELECTED_CONFIGS_OUTPUT)
    parser.add_argument("--loss-output", type=Path, default=DEFAULT_LOSS_OUTPUT)
    parser.add_argument("--stability-output", type=Path, default=DEFAULT_STABILITY_OUTPUT)
    parser.add_argument("--seed-stability-output", type=Path, default=DEFAULT_SEED_STABILITY_OUTPUT)
    parser.add_argument("--seed-stability-plot-output", type=Path, default=DEFAULT_SEED_STABILITY_PLOT_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    parser.add_argument("--text-column", default="text_for_nlp")
    parser.add_argument("--label-column", default="sentiment_label")
    parser.add_argument("--train-size", type=float, default=0.6)
    parser.add_argument("--validation-size", type=float, default=0.2)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--min-df", type=int, default=2)
    parser.add_argument("--max-df", type=float, default=0.98)
    parser.add_argument("--max-features", type=int, default=5000)
    parser.add_argument("--epochs", type=int, default=80)
    parser.add_argument("--sklearn-c", type=float, default=1.0)
    parser.add_argument("--sklearn-l2", type=float, default=0.0001)
    parser.add_argument("--convergence-tol", type=float, default=1e-4)
    parser.add_argument("--convergence-patience", type=int, default=5)
    parser.add_argument("--drop-duplicate-text", action="store_true")
    return parser.parse_args()


def validate_split_sizes(args: argparse.Namespace) -> None:
    split_sum = args.train_size + args.validation_size + args.test_size
    if not np.isclose(split_sum, 1.0):
        raise ValueError(
            "train-size, validation-size, and test-size must sum to 1.0; "
            f"got {split_sum:.6f}."
        )
    for name in ["train_size", "validation_size", "test_size"]:
        value = getattr(args, name)
        if value <= 0 or value >= 1:
            raise ValueError(f"{name} must be between 0 and 1; got {value}.")


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
    X_validation,
    y_train: np.ndarray,
    y_validation_labels: np.ndarray,
    tol: float,
    patience: int,
) -> tuple[dict, pd.DataFrame]:
    start = time.perf_counter()
    weights, bias, losses = train_for_config(config, X_train, y_train)
    runtime_seconds = time.perf_counter() - start

    y_pred = predict_labels(X_validation, weights, bias)
    probabilities = predict_proba(X_validation, weights, bias)
    result = {
        **asdict(config),
        "final_training_loss": losses[-1],
        "final_validation_loss": compute_loss(
            X_validation,
            (y_validation_labels == POSITIVE_LABEL).astype(np.float64),
            weights,
            bias,
            config.l2,
        ),
        "initial_training_loss": losses[0],
        "loss_reduction": losses[0] - losses[-1],
        "loss_roughness": loss_roughness(losses),
        "convergence_epoch": convergence_epoch(losses, tol, patience),
        "best_loss_epoch": int(np.argmin(losses) + 1),
        "epoch_to_90pct_loss_reduction": epoch_to_loss_reduction_fraction(losses, fraction=0.9),
        "runtime_seconds": runtime_seconds,
        "validation_accuracy": accuracy_score(y_validation_labels, y_pred),
        "validation_macro_f1": f1_score(y_validation_labels, y_pred, average="macro"),
        "validation_weighted_f1": f1_score(y_validation_labels, y_pred, average="weighted"),
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
        mean_validation_loss=("final_validation_loss", "mean"),
        std_validation_loss=("final_validation_loss", "std"),
        mean_runtime_seconds=("runtime_seconds", "mean"),
        std_runtime_seconds=("runtime_seconds", "std"),
        mean_validation_accuracy=("validation_accuracy", "mean"),
        std_validation_accuracy=("validation_accuracy", "std"),
        mean_validation_macro_f1=("validation_macro_f1", "mean"),
        std_validation_macro_f1=("validation_macro_f1", "std"),
        mean_validation_weighted_f1=("validation_weighted_f1", "mean"),
        std_validation_weighted_f1=("validation_weighted_f1", "std"),
    ).reset_index()


def make_seed_stability_summary(results: pd.DataFrame) -> pd.DataFrame:
    stability = results[
        (results["scenario"] == "stability_across_seeds")
        & (results["optimizer"].isin(CUSTOM_OPTIMIZERS))
    ].copy()
    if stability.empty:
        return pd.DataFrame(
            columns=[
                "optimizer",
                "number_of_seeds",
                "mean_accuracy",
                "std_accuracy",
                "mean_macro_f1",
                "std_macro_f1",
                "lower_ci_macro_f1",
                "upper_ci_macro_f1",
                "mean_weighted_f1",
                "std_weighted_f1",
                "mean_runtime",
                "std_runtime",
                "mean_final_loss",
                "std_final_loss",
            ]
        )

    summary = (
        stability.groupby("optimizer", dropna=False)
        .agg(
            number_of_seeds=("seed", "nunique"),
            mean_accuracy=("validation_accuracy", "mean"),
            std_accuracy=("validation_accuracy", "std"),
            mean_macro_f1=("validation_macro_f1", "mean"),
            std_macro_f1=("validation_macro_f1", "std"),
            mean_weighted_f1=("validation_weighted_f1", "mean"),
            std_weighted_f1=("validation_weighted_f1", "std"),
            mean_runtime=("runtime_seconds", "mean"),
            std_runtime=("runtime_seconds", "std"),
            mean_final_loss=("final_training_loss", "mean"),
            std_final_loss=("final_training_loss", "std"),
        )
        .reset_index()
    )
    margin = 1.96 * summary["std_macro_f1"] / np.sqrt(summary["number_of_seeds"])
    summary["lower_ci_macro_f1"] = summary["mean_macro_f1"] - margin
    summary["upper_ci_macro_f1"] = summary["mean_macro_f1"] + margin
    ordered_columns = [
        "optimizer",
        "number_of_seeds",
        "mean_accuracy",
        "std_accuracy",
        "mean_macro_f1",
        "std_macro_f1",
        "lower_ci_macro_f1",
        "upper_ci_macro_f1",
        "mean_weighted_f1",
        "std_weighted_f1",
        "mean_runtime",
        "std_runtime",
        "mean_final_loss",
        "std_final_loss",
    ]
    return summary[ordered_columns].sort_values("mean_macro_f1", ascending=False)


def save_seed_stability_boxplot(results: pd.DataFrame, output: Path) -> bool:
    stability = results[
        (results["scenario"] == "stability_across_seeds")
        & (results["optimizer"].isin(CUSTOM_OPTIMIZERS))
    ].copy()
    if stability.empty:
        return False

    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    optimizers = sorted(stability["optimizer"].unique())
    colors = {"Mini-batch GD": "#ff7f0e", "SGD": "#2ca02c"}
    fig, ax = plt.subplots(figsize=(7.5, 5))
    for x_pos, optimizer in enumerate(optimizers, start=1):
        group = stability.loc[stability["optimizer"] == optimizer].sort_values("seed")
        values = group["validation_macro_f1"].to_numpy()
        seeds = group["seed"].to_numpy()
        jitter = np.linspace(-0.08, 0.08, len(values)) if len(values) > 1 else np.array([0.0])
        ax.scatter(
            np.full(len(values), x_pos) + jitter,
            values,
            s=55,
            color=colors.get(optimizer, "#1f77b4"),
            alpha=0.75,
            edgecolor="black",
            linewidth=0.5,
            zorder=3,
        )
        for point_x, point_y, seed in zip(np.full(len(values), x_pos) + jitter, values, seeds):
            ax.annotate(
                int(seed),
                (point_x, point_y),
                xytext=(0, 7),
                textcoords="offset points",
                ha="center",
                fontsize=7,
                alpha=0.8,
            )
        mean = float(np.mean(values))
        std = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
        ci = 1.96 * std / np.sqrt(len(values)) if len(values) > 1 else 0.0
        ax.errorbar(
            x_pos,
            mean,
            yerr=ci,
            fmt="D",
            color="black",
            ecolor="black",
            elinewidth=1.8,
            capsize=7,
            markersize=7,
            zorder=4,
            label="Mean +/- 95% CI" if x_pos == 1 else None,
        )
    ax.set_xticks(np.arange(1, len(optimizers) + 1), optimizers)
    ax.set_title("Seed Stability: Validation Macro F1 by Optimizer")
    ax.set_xlabel("Optimizer")
    ax.set_ylabel("Validation macro F1")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return True


def split_train_validation_test(
    dataset: pd.DataFrame,
    args: argparse.Namespace,
) -> tuple[pd.Series, pd.Series, pd.Series, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    validate_split_sizes(args)
    text = dataset[args.text_column]
    labels = dataset[args.label_column].to_numpy()
    binary = (labels == POSITIVE_LABEL).astype(np.float64)

    X_train_validation_text, X_test_text, y_train_validation, y_test, labels_train_validation, labels_test = (
        train_test_split(
            text,
            binary,
            labels,
            test_size=args.test_size,
            random_state=args.random_state,
            stratify=labels,
        )
    )
    validation_fraction_of_remaining = args.validation_size / (args.train_size + args.validation_size)
    X_train_text, X_validation_text, y_train, y_validation, labels_train, labels_validation = train_test_split(
        X_train_validation_text,
        y_train_validation,
        labels_train_validation,
        test_size=validation_fraction_of_remaining,
        random_state=args.random_state,
        stratify=labels_train_validation,
    )
    return (
        X_train_text,
        X_validation_text,
        X_test_text,
        y_train,
        y_validation,
        y_test,
        labels_train,
        labels_validation,
        labels_test,
    )


def select_configs(validation_results: pd.DataFrame) -> pd.DataFrame:
    selected_rows = []
    sort_columns = ["validation_macro_f1", "runtime_seconds", "final_validation_loss"]
    ascending = [False, True, True]
    custom_results = validation_results[validation_results["optimizer"].isin(CUSTOM_OPTIMIZERS)].copy()

    for optimizer, rows in custom_results.groupby("optimizer"):
        best = rows.sort_values(sort_columns, ascending=ascending).iloc[0].copy()
        best["selection_scope"] = "best_for_optimizer"
        selected_rows.append(best)

    overall = custom_results.sort_values(sort_columns, ascending=ascending).iloc[0].copy()
    overall["selection_scope"] = "best_overall"
    selected_rows.append(overall)

    selected = pd.DataFrame(selected_rows)
    ordered_columns = [
        "selection_scope",
        "scenario",
        "optimizer",
        "learning_rate",
        "batch_size",
        "l2",
        "seed",
        "epochs",
        "validation_accuracy",
        "validation_macro_f1",
        "validation_weighted_f1",
        "final_training_loss",
        "final_validation_loss",
        "runtime_seconds",
        "loss_roughness",
        "convergence_epoch",
        "epoch_to_90pct_loss_reduction",
    ]
    return selected[ordered_columns]


def evaluate_selected_configs(
    selected_configs: pd.DataFrame,
    X_train,
    X_test,
    y_train: np.ndarray,
    y_test_labels: np.ndarray,
) -> pd.DataFrame:
    final_results = []
    evaluation_configs = (
        selected_configs.assign(batch_size_key=selected_configs["batch_size"].fillna(-1))
        .groupby(["scenario", "optimizer", "learning_rate", "batch_size_key", "l2", "seed", "epochs"], dropna=False)
        .agg(
            selection_scope=("selection_scope", lambda values: ";".join(values.astype(str))),
            batch_size=("batch_size", "first"),
            validation_macro_f1=("validation_macro_f1", "first"),
            validation_accuracy=("validation_accuracy", "first"),
            validation_weighted_f1=("validation_weighted_f1", "first"),
        )
        .reset_index()
        .drop(columns=["batch_size_key"])
    )
    for _, row in evaluation_configs.iterrows():
        config = ExperimentConfig(
            scenario=str(row["scenario"]),
            optimizer=str(row["optimizer"]),
            learning_rate=float(row["learning_rate"]),
            batch_size=None if pd.isna(row["batch_size"]) else int(row["batch_size"]),
            l2=float(row["l2"]),
            seed=int(row["seed"]),
            epochs=int(row["epochs"]),
        )
        start = time.perf_counter()
        weights, bias, losses = train_for_config(config, X_train, y_train)
        runtime_seconds = time.perf_counter() - start
        y_pred = predict_labels(X_test, weights, bias)
        probabilities = predict_proba(X_test, weights, bias)
        final_results.append(
            {
                "selection_scope": row["selection_scope"],
                **asdict(config),
                "validation_macro_f1": row["validation_macro_f1"],
                "validation_accuracy": row["validation_accuracy"],
                "validation_weighted_f1": row["validation_weighted_f1"],
                "test_macro_f1": f1_score(y_test_labels, y_pred, average="macro"),
                "test_accuracy": accuracy_score(y_test_labels, y_pred),
                "test_weighted_f1": f1_score(y_test_labels, y_pred, average="weighted"),
                "runtime_seconds": runtime_seconds,
                "final_loss": losses[-1],
                "final_test_loss": compute_loss(
                    X_test,
                    (y_test_labels == POSITIVE_LABEL).astype(np.float64),
                    weights,
                    bias,
                    config.l2,
                ),
                "positive_prediction_rate": float(np.mean(probabilities >= 0.5)),
            }
        )
    return pd.DataFrame(final_results)


def evaluate_sklearn_logreg(
    X_train,
    X_validation,
    X_test,
    y_train: np.ndarray,
    y_validation_labels: np.ndarray,
    y_test_labels: np.ndarray,
    args: argparse.Namespace,
) -> tuple[dict, dict]:
    start = time.perf_counter()
    model = LogisticRegression(
        C=args.sklearn_c,
        solver="lbfgs",
        max_iter=2000,
        random_state=args.random_state,
    )
    model.fit(X_train, y_train.astype(int))
    runtime_seconds = time.perf_counter() - start

    weights = np.asarray(model.coef_).ravel()
    bias = float(model.intercept_[0])
    y_validation_binary = (y_validation_labels == POSITIVE_LABEL).astype(np.float64)
    y_test_binary = (y_test_labels == POSITIVE_LABEL).astype(np.float64)
    validation_probabilities = model.predict_proba(X_validation)[:, list(model.classes_).index(1)]
    test_probabilities = model.predict_proba(X_test)[:, list(model.classes_).index(1)]
    validation_pred = np.where(validation_probabilities >= 0.5, "positive", "negative")
    test_pred = np.where(test_probabilities >= 0.5, "positive", "negative")

    common = {
        "scenario": "reference_baseline",
        "optimizer": "sklearn LogisticRegression",
        "learning_rate": np.nan,
        "batch_size": np.nan,
        "l2": args.sklearn_l2,
        "seed": args.random_state,
        "epochs": int(np.max(model.n_iter_)),
        "runtime_seconds": runtime_seconds,
    }
    validation_result = {
        **common,
        "final_training_loss": compute_loss(X_train, y_train, weights, bias, args.sklearn_l2),
        "final_validation_loss": compute_loss(X_validation, y_validation_binary, weights, bias, args.sklearn_l2),
        "initial_training_loss": np.nan,
        "loss_reduction": np.nan,
        "loss_roughness": np.nan,
        "convergence_epoch": np.nan,
        "best_loss_epoch": np.nan,
        "epoch_to_90pct_loss_reduction": np.nan,
        "validation_accuracy": accuracy_score(y_validation_labels, validation_pred),
        "validation_macro_f1": f1_score(y_validation_labels, validation_pred, average="macro"),
        "validation_weighted_f1": f1_score(y_validation_labels, validation_pred, average="weighted"),
        "positive_prediction_rate": float(np.mean(validation_probabilities >= 0.5)),
    }
    final_result = {
        "selection_scope": "reference_baseline",
        **common,
        "validation_macro_f1": validation_result["validation_macro_f1"],
        "validation_accuracy": validation_result["validation_accuracy"],
        "validation_weighted_f1": validation_result["validation_weighted_f1"],
        "test_macro_f1": f1_score(y_test_labels, test_pred, average="macro"),
        "test_accuracy": accuracy_score(y_test_labels, test_pred),
        "test_weighted_f1": f1_score(y_test_labels, test_pred, average="weighted"),
        "final_loss": validation_result["final_training_loss"],
        "final_test_loss": compute_loss(X_test, y_test_binary, weights, bias, args.sklearn_l2),
        "positive_prediction_rate": float(np.mean(test_probabilities >= 0.5)),
    }
    return validation_result, final_result


def dataframe_records_for_json(df: pd.DataFrame) -> list[dict]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def main() -> None:
    args = parse_args()
    dataset = build_dataset(args)
    (
        X_train_text,
        X_validation_text,
        X_test_text,
        y_train,
        _,
        _,
        y_train_labels,
        y_validation_labels,
        y_test_labels,
    ) = split_train_validation_test(
        dataset,
        args,
    )

    vectorizer = make_vectorizer(args)
    X_train = vectorizer.fit_transform(X_train_text)
    X_validation = vectorizer.transform(X_validation_text)
    X_test = vectorizer.transform(X_test_text)

    results = []
    loss_frames = []
    configs = build_experiment_grid(args.epochs)

    for idx, config in enumerate(configs, start=1):
        result, loss_rows = run_one_experiment(
            config,
            X_train,
            X_validation,
            y_train,
            y_validation_labels,
            tol=args.convergence_tol,
            patience=args.convergence_patience,
        )
        results.append(result)
        loss_frames.append(loss_rows)
        print(
            f"[{idx}/{len(configs)}] {config.scenario} | {config.optimizer} "
            f"lr={config.learning_rate} batch={config.batch_size} l2={config.l2} seed={config.seed}: "
            f"loss={result['final_training_loss']:.4f}, "
            f"validation_macro_f1={result['validation_macro_f1']:.4f}, "
            f"runtime={result['runtime_seconds']:.2f}s"
        )

    sklearn_validation_result, sklearn_final_result = evaluate_sklearn_logreg(
        X_train,
        X_validation,
        X_test,
        y_train,
        y_validation_labels,
        y_test_labels,
        args,
    )
    print(
        "sklearn LogisticRegression: "
        f"validation_macro_f1={sklearn_validation_result['validation_macro_f1']:.4f}, "
        f"test_macro_f1={sklearn_final_result['test_macro_f1']:.4f}, "
        f"runtime={sklearn_validation_result['runtime_seconds']:.2f}s"
    )
    results.append(sklearn_validation_result)

    results_frame = pd.DataFrame(results)
    loss_history = pd.concat(loss_frames, ignore_index=True)
    stability_summary = make_stability_summary(results_frame)
    seed_stability_summary = make_seed_stability_summary(results_frame)
    selected_configs = select_configs(results_frame)
    final_test_results = evaluate_selected_configs(selected_configs, X_train, X_test, y_train, y_test_labels)
    final_test_results = pd.concat([final_test_results, pd.DataFrame([sklearn_final_result])], ignore_index=True)
    seed_stability_plot_saved = save_seed_stability_boxplot(results_frame, args.seed_stability_plot_output)

    results_frame.to_csv(args.validation_results_output, index=False)
    loss_history.to_csv(args.loss_output, index=False)
    stability_summary.to_csv(args.stability_output, index=False)
    seed_stability_summary.to_csv(args.seed_stability_output, index=False)
    selected_configs.to_csv(args.selected_configs_output, index=False)
    final_test_results.to_csv(args.final_test_results_output, index=False)

    report = {
        "input_file": str(args.input),
        "text_column": args.text_column,
        "label_column": args.label_column,
        "train_rows": int(X_train.shape[0]),
        "validation_rows": int(X_validation.shape[0]),
        "test_rows": int(X_test.shape[0]),
        "split": {
            "train_size": args.train_size,
            "validation_size": args.validation_size,
            "test_size": args.test_size,
            "random_state": args.random_state,
            "stratified_by": args.label_column,
        },
        "feature_count": int(X_train.shape[1]),
        "epochs_per_run": args.epochs,
        "convergence_tol": args.convergence_tol,
        "convergence_patience": args.convergence_patience,
        "scenarios": sorted(results_frame["scenario"].unique().tolist()),
        "selection_method": (
            "Configurations are selected by validation_macro_f1, with runtime_seconds "
            "and final_validation_loss as tie-breakers. The held-out test set is used "
            "only after selection. sklearn LogisticRegression is reported as a "
            "reference baseline and is not used to replace the custom optimizer comparison."
        ),
        "sklearn_baseline": {
            "model": "LogisticRegression",
            "solver": "lbfgs",
            "C": args.sklearn_c,
            "l2_used_for_reported_objective": args.sklearn_l2,
            "max_iter": 2000,
        },
        "label_distribution": {
            "train": pd.Series(y_train_labels).value_counts().to_dict(),
            "validation": pd.Series(y_validation_labels).value_counts().to_dict(),
            "test": pd.Series(y_test_labels).value_counts().to_dict(),
        },
        "selected_configs": dataframe_records_for_json(selected_configs),
        "final_test_results": dataframe_records_for_json(final_test_results),
        "seed_stability_summary": dataframe_records_for_json(seed_stability_summary),
        "output_files": {
            "validation_results": str(args.validation_results_output),
            "loss_history": str(args.loss_output),
            "stability_summary": str(args.stability_output),
            "seed_stability_summary": str(args.seed_stability_output),
            "seed_stability_plot": str(args.seed_stability_plot_output)
            if seed_stability_plot_saved
            else None,
            "selected_configs": str(args.selected_configs_output),
            "final_test_results": str(args.final_test_results_output),
        },
    }
    args.report_output.write_text(json.dumps(report, indent=2))

    print(f"Saved validation optimizer results to: {args.validation_results_output}")
    print(f"Saved validation loss history to: {args.loss_output}")
    print(f"Saved validation stability summary to: {args.stability_output}")
    print(f"Saved seed stability summary to: {args.seed_stability_output}")
    if seed_stability_plot_saved:
        print(f"Saved seed stability plot to: {args.seed_stability_plot_output}")
    else:
        print("Skipped seed stability plot because no seed-stability data or matplotlib was unavailable.")
    print(f"Saved selected optimizer configs to: {args.selected_configs_output}")
    print(f"Saved final test optimizer results to: {args.final_test_results_output}")
    print(f"Saved validation optimizer report to: {args.report_output}")


if __name__ == "__main__":
    main()
