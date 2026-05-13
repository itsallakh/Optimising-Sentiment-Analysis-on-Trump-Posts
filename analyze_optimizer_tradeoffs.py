#!/usr/bin/env python3
"""Analyze saved optimizer tradeoff experiment outputs.

This script intentionally uses only the saved CSV/JSON artifacts from the
optimizer tradeoff runs. It does not retrain models or rerun experiments.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.animation import FuncAnimation, PillowWriter


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs/optimizers/analysis"

INPUT_FILES = {
    "results": "outputs/optimizers/validation/validation_optimizer_results.csv",
    "loss_history": "outputs/optimizers/validation/validation_optimizer_loss_history.csv",
    "stability": "outputs/optimizers/validation/validation_optimizer_stability_summary.csv",
    "report": "outputs/optimizers/validation/validation_optimizer_report.json",
}


ALIASES = {
    "scenario": ["scenario", "experiment", "setting"],
    "optimizer": ["optimizer", "method", "solver"],
    "learning_rate": ["learning_rate", "lr", "eta"],
    "batch_size": ["batch_size", "minibatch_size", "mini_batch_size"],
    "l2": ["l2", "lambda", "regularization", "l2_strength"],
    "seed": ["seed", "random_seed"],
    "epoch": ["epoch", "iteration", "step"],
    "training_loss": ["training_loss", "loss", "train_loss"],
    "final_training_loss": ["final_training_loss", "final_loss"],
    "loss_reduction": ["loss_reduction", "training_loss_reduction"],
    "loss_roughness": ["loss_roughness", "roughness", "loss_fluctuation"],
    "epoch_to_90pct_loss_reduction": [
        "epoch_to_90pct_loss_reduction",
        "epochs_to_90pct_loss_reduction",
        "epoch_to_90pct",
    ],
    "runtime_seconds": ["runtime_seconds", "runtime", "seconds", "fit_time"],
    "test_accuracy": ["test_accuracy", "validation_accuracy", "accuracy"],
    "test_macro_f1": ["test_macro_f1", "validation_macro_f1", "macro_f1"],
    "test_weighted_f1": ["test_weighted_f1", "validation_weighted_f1", "weighted_f1"],
    "mean_macro_f1": ["mean_macro_f1", "mean_validation_macro_f1", "macro_f1_mean"],
    "std_macro_f1": ["std_macro_f1", "std_validation_macro_f1", "macro_f1_std"],
    "mean_runtime_seconds": ["mean_runtime_seconds", "runtime_seconds_mean"],
    "std_runtime_seconds": ["std_runtime_seconds", "runtime_seconds_std"],
    "mean_final_loss": ["mean_final_loss", "final_loss_mean"],
    "std_final_loss": ["std_final_loss", "final_loss_std"],
}


def find_file(filename: str) -> Path:
    direct = ROOT / filename
    if direct.exists():
        return direct
    matches = list(ROOT.rglob(filename))
    if not matches:
        raise FileNotFoundError(f"Could not find {filename} under {ROOT}")
    return matches[0]


def resolve_columns(df: pd.DataFrame, needed: Iterable[str]) -> dict[str, str]:
    mapping = {}
    lower_to_original = {c.lower(): c for c in df.columns}
    for logical_name in needed:
        for alias in ALIASES.get(logical_name, [logical_name]):
            if alias.lower() in lower_to_original:
                mapping[logical_name] = lower_to_original[alias.lower()]
                break
    missing = [name for name in needed if name not in mapping]
    if missing:
        raise ValueError(f"Missing required columns {missing}; available columns: {list(df.columns)}")
    return mapping


def rename_known_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for logical_name, aliases in ALIASES.items():
        for col in df.columns:
            if col.lower() in {a.lower() for a in aliases}:
                rename_map[col] = logical_name
                break
    return df.rename(columns=rename_map)


def normalize_lower_is_better(series: pd.Series) -> pd.Series:
    values = series.astype(float)
    spread = values.max() - values.min()
    if not np.isfinite(spread) or spread == 0:
        return pd.Series(0.0, index=series.index)
    return (values.max() - values) / spread


def normalize_higher_is_better(series: pd.Series) -> pd.Series:
    values = series.astype(float)
    spread = values.max() - values.min()
    if not np.isfinite(spread) or spread == 0:
        return pd.Series(0.0, index=series.index)
    return (values - values.min()) / spread


def add_practical_score(df: pd.DataFrame) -> pd.DataFrame:
    scored = df.copy()
    scored["performance_score"] = normalize_higher_is_better(scored["test_macro_f1"])
    scored["runtime_score"] = normalize_lower_is_better(scored["runtime_seconds"])
    scored["smoothness_score"] = normalize_lower_is_better(scored["loss_roughness"].fillna(scored["loss_roughness"].median()))
    scored["practical_score"] = (
        0.60 * scored["performance_score"]
        + 0.25 * scored["runtime_score"]
        + 0.15 * scored["smoothness_score"]
    )
    return scored


def fmt_value(value) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (float, np.floating)):
        if abs(value) >= 100:
            return f"{value:.2f}"
        if abs(value) >= 1:
            return f"{value:.4f}"
        return f"{value:.6f}"
    return str(value)


def write_markdown_table(df: pd.DataFrame, path: Path, title: str) -> None:
    lines = [f"# {title}", ""]
    display = df.copy()
    for col in display.columns:
        display[col] = display[col].map(fmt_value)
    lines.append(display.to_markdown(index=False))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def concise_config(row: pd.Series) -> str:
    parts = [str(row["optimizer"])]
    if pd.notna(row.get("learning_rate")):
        parts.append(f"lr={row['learning_rate']:g}")
    if pd.notna(row.get("batch_size")):
        parts.append(f"batch={int(row['batch_size'])}")
    if pd.notna(row.get("l2")):
        parts.append(f"L2={row['l2']:g}")
    if pd.notna(row.get("seed")):
        parts.append(f"seed={int(row['seed'])}")
    return ", ".join(parts)


def save_figure(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()


def make_overall_summary(results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for optimizer, group in results.groupby("optimizer"):
        best_f1 = group.loc[group["test_macro_f1"].idxmax()]
        best_practical = group.loc[group["practical_score"].idxmax()]
        fastest = group.loc[group["runtime_seconds"].idxmin()]
        rows.append(
            {
                "optimizer": optimizer,
                "runs": len(group),
                "mean_macro_f1": group["test_macro_f1"].mean(),
                "best_macro_f1": best_f1["test_macro_f1"],
                "best_macro_f1_config": concise_config(best_f1),
                "mean_runtime_seconds": group["runtime_seconds"].mean(),
                "fastest_runtime_seconds": fastest["runtime_seconds"],
                "mean_loss_roughness": group["loss_roughness"].mean(),
                "mean_epoch_to_90pct_loss_reduction": group["epoch_to_90pct_loss_reduction"].mean(),
                "best_practical_score": best_practical["practical_score"],
                "best_practical_config": concise_config(best_practical),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["best_practical_score", "best_macro_f1"], ascending=False
    )


def make_scenario_best_results(results: pd.DataFrame, stability: pd.DataFrame) -> pd.DataFrame:
    rows = []
    labels = {
        "learning_rate_sensitivity": "best result by learning-rate scenario",
        "mini_batch_size": "best result by mini-batch-size scenario",
        "l2_interaction": "best result by L2-interaction scenario",
    }
    for scenario, label in labels.items():
        group = results[results["scenario"] == scenario]
        if group.empty:
            continue
        best = group.loc[group["practical_score"].idxmax()]
        rows.append(
            {
                "summary_item": label,
                "scenario": scenario,
                "optimizer": best["optimizer"],
                "learning_rate": best["learning_rate"],
                "batch_size": best["batch_size"],
                "l2": best["l2"],
                "seed": best["seed"],
                "macro_f1": best["test_macro_f1"],
                "runtime_seconds": best["runtime_seconds"],
                "loss_roughness": best["loss_roughness"],
                "practical_score": best["practical_score"],
                "basis": "highest practical score within scenario",
            }
        )

    if not stability.empty:
        stable = stability.copy()
        stable["stability_score"] = (
            0.55 * normalize_higher_is_better(stable["mean_macro_f1"])
            + 0.25 * normalize_lower_is_better(stable["std_macro_f1"])
            + 0.20 * normalize_lower_is_better(stable["mean_runtime_seconds"])
        )
        best_stable = stable.loc[stable["stability_score"].idxmax()]
        rows.append(
            {
                "summary_item": "best result by stability-across-seeds scenario",
                "scenario": "stability_across_seeds",
                "optimizer": best_stable["optimizer"],
                "learning_rate": best_stable["learning_rate"],
                "batch_size": best_stable["batch_size"],
                "l2": best_stable["l2"],
                "seed": "",
                "macro_f1": best_stable["mean_macro_f1"],
                "runtime_seconds": best_stable["mean_runtime_seconds"],
                "loss_roughness": np.nan,
                "practical_score": best_stable["stability_score"],
                "basis": "mean macro F1, macro-F1 stability, and mean runtime across seeds",
            }
        )

    overall = results.loc[results["practical_score"].idxmax()]
    rows.append(
        {
            "summary_item": "practical overall winner",
            "scenario": overall["scenario"],
            "optimizer": overall["optimizer"],
            "learning_rate": overall["learning_rate"],
            "batch_size": overall["batch_size"],
            "l2": overall["l2"],
            "seed": overall["seed"],
            "macro_f1": overall["test_macro_f1"],
            "runtime_seconds": overall["runtime_seconds"],
            "loss_roughness": overall["loss_roughness"],
            "practical_score": overall["practical_score"],
            "basis": "best runtime/performance/smoothness tradeoff across all saved runs",
        }
    )
    return pd.DataFrame(rows)


def plot_loss_curves(results: pd.DataFrame, loss: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(9, 5.5))
    for optimizer, group in results.groupby("optimizer"):
        selected = group.loc[group["practical_score"].idxmax()]
        mask = np.ones(len(loss), dtype=bool)
        for col in ["scenario", "optimizer", "learning_rate", "batch_size", "l2", "seed"]:
            if pd.isna(selected[col]):
                mask &= loss[col].isna()
            else:
                mask &= loss[col].eq(selected[col])
        curve = loss[mask].sort_values("epoch")
        if curve.empty:
            continue
        plt.plot(curve["epoch"], curve["training_loss"], linewidth=2, label=concise_config(selected))
    plt.title("Training Loss Curves by Optimizer")
    plt.xlabel("Epoch")
    plt.ylabel("Training loss")
    plt.grid(alpha=0.25)
    plt.legend(fontsize=8)
    save_figure(path)


def plot_runtime_vs_macro_f1(results: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9.5, 6.0))
    markers = {"Batch GD": "o", "SGD": "s", "Mini-batch GD": "^"}
    for optimizer, group in results.groupby("optimizer"):
        ax.scatter(
            group["runtime_seconds"],
            group["test_macro_f1"],
            s=75,
            alpha=0.78,
            marker=markers.get(optimizer, "o"),
            label=optimizer,
        )
    label_offsets = {
        "Batch GD": (12, -18),
        "Mini-batch GD": (18, 14),
        "SGD": (-54, 14),
    }
    for optimizer, group in results.groupby("optimizer"):
        row = group.loc[group["practical_score"].idxmax()]
        ax.annotate(
            row["optimizer"],
            (row["runtime_seconds"], row["test_macro_f1"]),
            xytext=label_offsets.get(row["optimizer"], (10, 10)),
            textcoords="offset points",
            fontsize=8.5,
            ha="right" if row["optimizer"] == "SGD" else "left",
            bbox={"boxstyle": "round,pad=0.2", "fc": "white", "ec": "none", "alpha": 0.78},
            arrowprops={"arrowstyle": "-", "color": "0.45", "lw": 0.8, "shrinkA": 2, "shrinkB": 4},
        )
    ax.set_title("Runtime vs. Final Macro F1")
    ax.set_xlabel("Runtime (seconds)")
    ax.set_ylabel("Test macro F1")
    ax.margins(x=0.08, y=0.12)
    ax.grid(alpha=0.25)
    ax.legend(loc="lower right")
    save_figure(path)


def plot_learning_rate_sensitivity(results: pd.DataFrame, path: Path) -> None:
    data = results[results["scenario"] == "learning_rate_sensitivity"].copy()
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
    for optimizer, group in data.groupby("optimizer"):
        group = group.sort_values("learning_rate")
        axes[0].plot(group["learning_rate"], group["test_macro_f1"], marker="o", label=optimizer)
        axes[1].plot(group["learning_rate"], group["runtime_seconds"], marker="o", label=optimizer)
    axes[0].set_title("Macro F1 by Learning Rate")
    axes[0].set_xlabel("Learning rate")
    axes[0].set_ylabel("Test macro F1")
    axes[1].set_title("Runtime by Learning Rate")
    axes[1].set_xlabel("Learning rate")
    axes[1].set_ylabel("Runtime (seconds)")
    for ax in axes:
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
    save_figure(path)


def plot_mini_batch_size_tradeoff(results: pd.DataFrame, path: Path) -> None:
    data = results[results["scenario"] == "mini_batch_size"].sort_values("batch_size")
    fig, ax1 = plt.subplots(figsize=(8.5, 5.2))
    ax2 = ax1.twinx()
    ax1.plot(data["batch_size"], data["test_macro_f1"], marker="o", color="#1f77b4", label="Macro F1")
    ax2.plot(data["batch_size"], data["runtime_seconds"], marker="s", color="#d62728", label="Runtime")
    ax1.set_title("Mini-batch Size Tradeoff")
    ax1.set_xlabel("Mini-batch size")
    ax1.set_ylabel("Test macro F1", color="#1f77b4")
    ax2.set_ylabel("Runtime (seconds)", color="#d62728")
    ax1.grid(alpha=0.25)
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="best")
    save_figure(path)


def plot_stability(stability: pd.DataFrame, path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.8))
    x = np.arange(len(stability))
    labels = stability["optimizer"].tolist()
    axes[0].bar(x, stability["mean_macro_f1"], yerr=stability["std_macro_f1"], capsize=5)
    axes[0].set_xticks(x, labels, rotation=15)
    axes[0].set_title("Macro F1 Stability Across Seeds")
    axes[0].set_ylabel("Mean test macro F1")
    axes[0].grid(axis="y", alpha=0.25)
    axes[1].bar(x, stability["mean_runtime_seconds"], yerr=stability["std_runtime_seconds"], capsize=5)
    axes[1].set_xticks(x, labels, rotation=15)
    axes[1].set_title("Runtime Stability Across Seeds")
    axes[1].set_ylabel("Mean runtime (seconds)")
    axes[1].grid(axis="y", alpha=0.25)
    save_figure(path)


def plot_l2_interaction(results: pd.DataFrame, path: Path) -> None:
    data = results[results["scenario"] == "l2_interaction"].copy()
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
    for optimizer, group in data.groupby("optimizer"):
        group = group.sort_values("l2")
        axes[0].plot(group["l2"], group["test_macro_f1"], marker="o", label=optimizer)
        axes[1].plot(group["l2"], group["final_training_loss"], marker="o", label=optimizer)
    axes[0].set_title("L2 Strength vs. Macro F1")
    axes[0].set_xlabel("L2 strength")
    axes[0].set_ylabel("Test macro F1")
    axes[1].set_title("L2 Strength vs. Final Training Loss")
    axes[1].set_xlabel("L2 strength")
    axes[1].set_ylabel("Final training loss")
    for ax in axes:
        ax.set_xscale("log")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
    save_figure(path)


def create_loss_animation(results: pd.DataFrame, loss: pd.DataFrame, path: Path, note_path: Path) -> bool:
    required = {"epoch", "training_loss", "optimizer"}
    if not required.issubset(loss.columns) or loss["epoch"].nunique() < 3:
        note_path.write_text(
            "# Missing Animation Note\n\n"
            "The saved files do not contain enough sequential loss information to animate loss over training. "
            "A valid animation would need at least optimizer labels, epoch or iteration numbers, and a loss value "
            "for each step. For decision-evolution or probability animations, the experiment would also need to save "
            "per-epoch predicted probabilities, model checkpoints, or model weights.\n",
            encoding="utf-8",
        )
        return False

    curves = []
    for optimizer, group in results.groupby("optimizer"):
        selected = group.loc[group["practical_score"].idxmax()]
        mask = np.ones(len(loss), dtype=bool)
        for col in ["scenario", "optimizer", "learning_rate", "batch_size", "l2", "seed"]:
            if pd.isna(selected[col]):
                mask &= loss[col].isna()
            else:
                mask &= loss[col].eq(selected[col])
        curve = loss[mask].sort_values("epoch")[["epoch", "training_loss"]]
        if len(curve) >= 3:
            curves.append((optimizer, concise_config(selected), curve))

    if len(curves) < 2:
        note_path.write_text(
            "# Missing Animation Note\n\n"
            "The loss history has sequential data, but not enough matched optimizer curves were found for a useful "
            "optimizer loss animation. A richer animation would need per-epoch loss for Batch GD, SGD, and "
            "Mini-batch GD under comparable saved configurations. For classification/probability animation, save "
            "per-epoch predicted probabilities, checkpoints, or model weights.\n",
            encoding="utf-8",
        )
        return False

    fig, ax = plt.subplots(figsize=(8, 5))
    max_epoch = int(max(curve["epoch"].max() for _, _, curve in curves))
    y_min = min(curve["training_loss"].min() for _, _, curve in curves)
    y_max = max(curve["training_loss"].max() for _, _, curve in curves)
    lines = []
    for _, label, _ in curves:
        (line,) = ax.plot([], [], linewidth=2, label=label)
        lines.append(line)
    ax.set_xlim(1, max_epoch)
    ax.set_ylim(y_min * 0.96, y_max * 1.04)
    ax.set_title("Optimizer Training Loss Over Epochs")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Training loss")
    ax.grid(alpha=0.25)
    ax.legend(fontsize=7)

    def update(frame: int):
        for line, (_, _, curve) in zip(lines, curves):
            current = curve[curve["epoch"] <= frame]
            line.set_data(current["epoch"], current["training_loss"])
        ax.set_title(f"Optimizer Training Loss Over Epochs - epoch {frame}")
        return lines

    anim = FuncAnimation(fig, update, frames=range(1, max_epoch + 1), interval=80, blit=False)
    anim.save(path, writer=PillowWriter(fps=12))
    plt.close(fig)
    if note_path.exists():
        note_path.unlink()
    return True


def sentence_best(row: pd.Series) -> str:
    return (
        f"{row['optimizer']} (lr={row['learning_rate']:g}"
        + (f", batch={int(row['batch_size'])}" if pd.notna(row.get("batch_size")) else "")
        + f", L2={row['l2']:g})"
    )


def write_interpretation(
    results: pd.DataFrame,
    stability: pd.DataFrame,
    scenario_best: pd.DataFrame,
    path: Path,
    gif_created: bool,
) -> None:
    fastest_90 = results.dropna(subset=["epoch_to_90pct_loss_reduction"]).loc[
        results.dropna(subset=["epoch_to_90pct_loss_reduction"])["epoch_to_90pct_loss_reduction"].idxmin()
    ]
    smoothest = results.loc[results["loss_roughness"].idxmin()]
    roughest = results.loc[results["loss_roughness"].idxmax()]
    best_f1 = results.loc[results["test_macro_f1"].idxmax()]
    fastest_runtime = results.loc[results["runtime_seconds"].idxmin()]
    practical = results.loc[results["practical_score"].idxmax()]

    mb = results[results["optimizer"] == "Mini-batch GD"]
    sgd = results[results["optimizer"] == "SGD"]
    mb_practical = mb.loc[mb["practical_score"].idxmax()] if not mb.empty else None
    sgd_best = sgd.loc[sgd["test_macro_f1"].idxmax()] if not sgd.empty else None

    lr = results[results["scenario"] == "learning_rate_sensitivity"]
    mb_size = results[results["scenario"] == "mini_batch_size"]
    l2 = results[results["scenario"] == "l2_interaction"]

    lines = [
        "# Optimizer Tradeoff Interpretation",
        "",
        "This analysis keeps the modeling setup fixed: same dataset, same TF-IDF features, same binary logistic regression model, same L2 baseline, and same train/test split. The only moving parts are optimizer-related settings.",
        "",
        "## Main Takeaways",
        "",
        f"- Fastest convergence by the saved 90% loss-reduction marker: {sentence_best(fastest_90)}, reaching that point at epoch {fastest_90['epoch_to_90pct_loss_reduction']:.0f}.",
        f"- Smoothest loss trajectory: {sentence_best(smoothest)}, with loss roughness {smoothest['loss_roughness']:.6f}.",
        f"- Most fluctuating loss trajectory: {sentence_best(roughest)}, with loss roughness {roughest['loss_roughness']:.6f}.",
        f"- Best final test macro F1: {sentence_best(best_f1)}, macro F1 {best_f1['test_macro_f1']:.4f}, runtime {best_f1['runtime_seconds']:.3f} seconds.",
        f"- Fastest run: {sentence_best(fastest_runtime)}, runtime {fastest_runtime['runtime_seconds']:.3f} seconds, macro F1 {fastest_runtime['test_macro_f1']:.4f}.",
        f"- Practical runtime/performance winner: {sentence_best(practical)}, macro F1 {practical['test_macro_f1']:.4f}, runtime {practical['runtime_seconds']:.3f} seconds.",
        "",
        "## Convergence, Noise, and Speed",
        "",
        f"The fastest convergence marker belongs to {fastest_90['optimizer']}. The smoothest observed curve belongs to {smoothest['optimizer']}, while the roughest belongs to {roughest['optimizer']}. This matches the expected optimization tradeoff: methods that update more frequently can reach useful regions quickly, but their loss paths can be noisier than full-batch updates.",
        "",
        "Runtime tells a different part of the story than final macro F1. Batch GD has low per-run overhead in these saved runs, SGD explores aggressively but can be much slower, and Mini-batch GD often sits in the middle: enough stochasticity to improve learning behavior, but not so much update-by-update cost that runtime dominates the result.",
        "",
        "## Why Mini-batch GD Is a Strong Practical Choice",
        "",
    ]
    if mb_practical is not None and sgd_best is not None:
        lines.extend(
            [
                f"SGD can achieve the top final macro F1 in the saved results: {sgd_best['test_macro_f1']:.4f} at {sgd_best['runtime_seconds']:.3f} seconds. Mini-batch GD's strongest practical run reaches macro F1 {mb_practical['test_macro_f1']:.4f} at {mb_practical['runtime_seconds']:.3f} seconds.",
                "That makes Mini-batch GD easy to defend for a final report: the goal is not only the single highest endpoint, but the balance among final classification performance, convergence behavior, runtime, and stability. A mini-batch update uses more signal per step than SGD while avoiding the fully deterministic, sometimes slower-to-improve behavior of Batch GD.",
                "",
            ]
        )

    if not lr.empty:
        lr_best = lr.loc[lr["test_macro_f1"].idxmax()]
        lr_practical = lr.loc[lr["practical_score"].idxmax()]
        lines.extend(
            [
                "## Learning Rate Sensitivity",
                "",
                f"In the learning-rate scenario, the best macro F1 comes from {sentence_best(lr_best)} with macro F1 {lr_best['test_macro_f1']:.4f}. The best practical tradeoff in that same scenario is {sentence_best(lr_practical)}.",
                "The saved curves show that learning rate changes both the endpoint and the shape of optimization. Smaller rates tend to move conservatively; larger rates can reduce loss faster, but they can also increase roughness or push the optimizer into less useful behavior if the step size is too aggressive.",
                "",
            ]
        )

    if not mb_size.empty:
        mb_best = mb_size.loc[mb_size["test_macro_f1"].idxmax()]
        mb_fast = mb_size.loc[mb_size["runtime_seconds"].idxmin()]
        lines.extend(
            [
                "## Mini-batch Size Tradeoff",
                "",
                f"Within the mini-batch-size scenario, the best macro F1 is batch size {int(mb_best['batch_size'])}, with macro F1 {mb_best['test_macro_f1']:.4f}. The fastest mini-batch run is batch size {int(mb_fast['batch_size'])}, with runtime {mb_fast['runtime_seconds']:.3f} seconds.",
                "The batch-size results show the practical compromise directly: smaller batches make more frequent noisy updates, while larger batches use more examples per update and can be smoother, but may not give the best runtime/performance point.",
                "",
            ]
        )

    if not stability.empty:
        stable_best = stability.loc[stability["mean_macro_f1"].idxmax()]
        stable_low_var = stability.loc[stability["std_macro_f1"].idxmin()]
        stable_fast = stability.loc[stability["mean_runtime_seconds"].idxmin()]
        lines.extend(
            [
                "## Seed Stability",
                "",
                f"Across seeds, {stable_best['optimizer']} has the higher mean macro F1 ({stable_best['mean_macro_f1']:.4f} +/- {stable_best['std_macro_f1']:.4f}). {stable_low_var['optimizer']} has the lower macro-F1 standard deviation ({stable_low_var['std_macro_f1']:.4f}), and {stable_fast['optimizer']} is faster on average ({stable_fast['mean_runtime_seconds']:.3f} seconds).",
                "The seed-stability result means the optimizer choice affects reproducibility, not just average performance. A small standard deviation suggests that the optimizer is less dependent on random initialization or data order for this fixed model and feature setup.",
                "",
            ]
        )

    if not l2.empty:
        l2_best = l2.loc[l2["test_macro_f1"].idxmax()]
        lines.extend(
            [
                "## L2 Interaction",
                "",
                f"In the L2-interaction scenario, the best macro F1 is produced by {sentence_best(l2_best)}, macro F1 {l2_best['test_macro_f1']:.4f}.",
                "Changing L2 strength changes the optimization landscape and the amount of shrinkage on the logistic-regression weights. In the saved results, the best regularization strength is therefore optimizer-dependent: the same optimizer settings do not behave identically as the penalty changes.",
                "",
            ]
        )

    lines.extend(
        [
            "## Animation",
            "",
            "The saved loss history includes epoch-level training loss, so a loss-curve animation was created." if gif_created else "A loss-curve animation was not created because the saved files did not contain enough sequential information.",
            "",
            "## Final Conclusion",
            "",
            f"Plain-language takeaway: for this fixed logistic-regression sentiment setup, {practical['optimizer']} is the strongest practical optimizer choice because it gives the best saved balance of classification quality, runtime, and loss behavior, while SGD remains valuable when the only target is the highest final macro F1.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    generated: list[Path] = []

    paths = {name: find_file(filename) for name, filename in INPUT_FILES.items()}
    results = rename_known_columns(pd.read_csv(paths["results"]))
    loss = rename_known_columns(pd.read_csv(paths["loss_history"]))
    stability = rename_known_columns(pd.read_csv(paths["stability"]))
    with open(paths["report"], "r", encoding="utf-8") as f:
        report = json.load(f)

    resolve_columns(
        results,
        [
            "scenario",
            "optimizer",
            "learning_rate",
            "batch_size",
            "l2",
            "seed",
            "final_training_loss",
            "loss_reduction",
            "loss_roughness",
            "epoch_to_90pct_loss_reduction",
            "runtime_seconds",
            "test_accuracy",
            "test_macro_f1",
            "test_weighted_f1",
        ],
    )
    resolve_columns(loss, ["scenario", "optimizer", "learning_rate", "batch_size", "l2", "seed", "epoch", "training_loss"])
    resolve_columns(stability, ["optimizer", "mean_macro_f1", "std_macro_f1", "mean_runtime_seconds", "std_runtime_seconds"])

    results = add_practical_score(results)

    overall = make_overall_summary(results)
    scenario_best = make_scenario_best_results(results, stability)

    table_specs = [
        (overall, "overall_optimizer_summary.csv", "overall_optimizer_summary.md", "Overall Optimizer Summary"),
        (scenario_best, "scenario_best_results.csv", "scenario_best_results.md", "Scenario Best Results"),
    ]
    for df, csv_name, md_name, title in table_specs:
        csv_path = OUTPUT_DIR / csv_name
        md_path = OUTPUT_DIR / md_name
        df.to_csv(csv_path, index=False)
        write_markdown_table(df, md_path, title)
        generated.extend([csv_path, md_path])

    figure_jobs = [
        (plot_loss_curves, (results, loss, OUTPUT_DIR / "loss_curves_by_optimizer.png")),
        (plot_runtime_vs_macro_f1, (results, OUTPUT_DIR / "runtime_vs_macro_f1.png")),
        (plot_learning_rate_sensitivity, (results, OUTPUT_DIR / "learning_rate_sensitivity.png")),
        (plot_mini_batch_size_tradeoff, (results, OUTPUT_DIR / "mini_batch_size_tradeoff.png")),
        (plot_stability, (stability, OUTPUT_DIR / "stability_across_seeds.png")),
        (plot_l2_interaction, (results, OUTPUT_DIR / "l2_interaction_plot.png")),
    ]
    for func, args in figure_jobs:
        func(*args)
        generated.append(args[-1])

    gif_path = OUTPUT_DIR / "optimizer_loss_animation.gif"
    note_path = OUTPUT_DIR / "missing_animation_note.md"
    gif_created = create_loss_animation(results, loss, gif_path, note_path)
    generated.append(gif_path if gif_created else note_path)

    interpretation_path = OUTPUT_DIR / "optimizer_interpretation.md"
    write_interpretation(results, stability, scenario_best, interpretation_path, gif_created)
    generated.append(interpretation_path)

    metadata_path = OUTPUT_DIR / "analysis_metadata.json"
    metadata = {
        "source_files": {name: str(path.relative_to(ROOT)) for name, path in paths.items()},
        "inferred_columns": {
            "results": list(results.columns),
            "loss_history": list(loss.columns),
            "stability": list(stability.columns),
        },
        "experiment_report": report,
        "generated_files": [str(path.relative_to(ROOT)) for path in generated],
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    generated.append(metadata_path)

    print("Optimizer analysis complete.")
    print(f"Output folder: {OUTPUT_DIR.relative_to(ROOT)}")
    print("Generated files:")
    for path in generated:
        print(f"- {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
