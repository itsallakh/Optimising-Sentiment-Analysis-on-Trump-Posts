from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RAW_DATA = ROOT / "data/raw/factbase_truthsocial_texts.csv"
SCRAPER_OUTPUT = ROOT / "factbase_truthsocial_texts.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Factbase sentiment optimization pipeline.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--from-scrape",
        action="store_true",
        help="Run the scraper first, then clean, preprocess, label, train, and analyze.",
    )
    group.add_argument(
        "--from-saved-data",
        action="store_true",
        help="Skip scraping and start from the saved raw CSV in data/raw/.",
    )
    return parser.parse_args()


def run_step(command: list[str]) -> None:
    print("\n$ " + " ".join(command), flush=True)
    env = os.environ.copy()
    env.setdefault("MPLCONFIGDIR", str(ROOT / ".cache/matplotlib"))
    subprocess.run(command, cwd=ROOT, check=True, env=env)


def ensure_directories() -> None:
    for path in [
        ROOT / "data/raw",
        ROOT / "data/processed",
        ROOT / "data/features",
        ROOT / "models",
        ROOT / "outputs/classification",
        ROOT / "outputs/optimizers/baseline",
        ROOT / "outputs/optimizers/tradeoff",
        ROOT / "outputs/optimizers/validation/figures",
        ROOT / "outputs/optimizers/analysis",
        ROOT / ".cache/matplotlib",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def run_scraper() -> None:
    if RAW_DATA.exists() and not SCRAPER_OUTPUT.exists():
        shutil.copy2(RAW_DATA, SCRAPER_OUTPUT)
        print(f"Copied existing raw data to scraper working file: {SCRAPER_OUTPUT.name}")

    run_step([sys.executable, "scrape_factbase.py"])

    if not SCRAPER_OUTPUT.exists():
        raise FileNotFoundError(f"Scraper did not create expected file: {SCRAPER_OUTPUT}")
    RAW_DATA.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(SCRAPER_OUTPUT), RAW_DATA)
    print(f"Saved scraper output to: {RAW_DATA.relative_to(ROOT)}")


def run_modeling_pipeline() -> None:
    if not RAW_DATA.exists():
        raise FileNotFoundError(
            "Saved raw data was not found. Run `python run_pipeline.py --from-scrape` first, "
            "or place the raw CSV at data/raw/factbase_truthsocial_texts.csv."
        )

    run_step(
        [
            sys.executable,
            "clean_factbase_dataset.py",
            "--input",
            "data/raw/factbase_truthsocial_texts.csv",
            "--output",
            "data/processed/factbase_truthsocial_texts_clean.csv",
        ]
    )
    run_step(
        [
            sys.executable,
            "prepare_tfidf_dataset.py",
            "--input",
            "data/processed/factbase_truthsocial_texts_clean.csv",
            "--nlp-ready-output",
            "data/processed/factbase_truthsocial_texts_nlp_ready.csv",
            "--tfidf-output",
            "data/features/factbase_truthsocial_texts_tfidf.npz",
            "--features-output",
            "data/features/factbase_truthsocial_texts_tfidf_features.csv",
            "--vectorizer-output",
            "data/features/factbase_truthsocial_tfidf_vectorizer.joblib",
            "--config-output",
            "data/features/factbase_truthsocial_tfidf_config.json",
        ]
    )
    run_step(
        [
            sys.executable,
            "label_sentiment_dataset.py",
            "--input",
            "data/processed/factbase_truthsocial_texts_nlp_ready.csv",
            "--output",
            "data/processed/factbase_truthsocial_texts_sentiment_labeled.csv",
            "--summary-output",
            "data/processed/factbase_truthsocial_texts_sentiment_label_summary.json",
            "--text-column",
            "text_clean",
        ]
    )
    run_step(
        [
            sys.executable,
            "train_sentiment_logreg.py",
            "--input",
            "data/processed/factbase_truthsocial_texts_sentiment_labeled.csv",
            "--model-output",
            "models/sentiment_logreg_pipeline.joblib",
            "--report-output",
            "outputs/classification/sentiment_logreg_report.json",
            "--predictions-output",
            "outputs/classification/sentiment_logreg_test_predictions.csv",
        ]
    )
    run_step(
        [
            sys.executable,
            "train_custom_optimizers.py",
            "--input",
            "data/processed/factbase_truthsocial_texts_sentiment_labeled.csv",
            "--results-output",
            "outputs/optimizers/baseline/optimizer_results.csv",
            "--loss-output",
            "outputs/optimizers/baseline/optimizer_loss_history.csv",
            "--report-output",
            "outputs/optimizers/baseline/optimizer_report.json",
            "--predictions-output",
            "outputs/optimizers/baseline/optimizer_test_predictions.csv",
            "--plot-output",
            "outputs/optimizers/baseline/optimizer_loss_curves.png",
        ]
    )
    run_step(
        [
            sys.executable,
            "optimizer_tradeoff_experiments.py",
            "--input",
            "data/processed/factbase_truthsocial_texts_sentiment_labeled.csv",
            "--validation-results-output",
            "outputs/optimizers/validation/validation_optimizer_results.csv",
            "--final-test-results-output",
            "outputs/optimizers/validation/final_test_optimizer_results.csv",
            "--selected-configs-output",
            "outputs/optimizers/validation/selected_optimizer_configs.csv",
            "--loss-output",
            "outputs/optimizers/validation/validation_optimizer_loss_history.csv",
            "--stability-output",
            "outputs/optimizers/validation/validation_optimizer_stability_summary.csv",
            "--seed-stability-output",
            "outputs/optimizers/validation/optimizer_seed_stability_summary.csv",
            "--seed-stability-plot-output",
            "outputs/optimizers/validation/figures/seed_stability_boxplot.png",
            "--report-output",
            "outputs/optimizers/validation/validation_optimizer_report.json",
        ]
    )
    run_step([sys.executable, "analyze_optimizer_tradeoffs.py"])


def main() -> None:
    args = parse_args()
    ensure_directories()
    if args.from_scrape:
        run_scraper()
    run_modeling_pipeline()
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
