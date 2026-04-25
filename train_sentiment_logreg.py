from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from joblib import dump
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline


DEFAULT_INPUT = Path("factbase_truthsocial_texts_sentiment_labeled.csv")
DEFAULT_MODEL_OUTPUT = Path("sentiment_logreg_pipeline.joblib")
DEFAULT_REPORT_OUTPUT = Path("sentiment_logreg_report.json")
DEFAULT_PREDICTIONS_OUTPUT = Path("sentiment_logreg_test_predictions.csv")

VALID_LABELS = {"positive", "negative"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and evaluate an L2-regularized binary logistic regression sentiment classifier."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--model-output", type=Path, default=DEFAULT_MODEL_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    parser.add_argument("--predictions-output", type=Path, default=DEFAULT_PREDICTIONS_OUTPUT)
    parser.add_argument("--text-column", default="text_for_nlp")
    parser.add_argument("--label-column", default="sentiment_label")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--min-df", type=int, default=2)
    parser.add_argument("--max-df", type=float, default=0.98)
    parser.add_argument("--max-features", type=int, default=5000)
    parser.add_argument("--c", type=float, default=1.0)
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


def main() -> None:
    args = parse_args()
    dataset = build_dataset(args)

    X_train, X_test, y_train, y_test, train_idx, test_idx = train_test_split(
        dataset[args.text_column],
        dataset[args.label_column],
        dataset.index,
        test_size=args.test_size,
        random_state=args.random_state,
        stratify=dataset[args.label_column],
    )

    pipeline = Pipeline(
        steps=[
            (
                "tfidf",
                TfidfVectorizer(
                    lowercase=False,
                    strip_accents="unicode",
                    ngram_range=(1, 2),
                    min_df=args.min_df,
                    max_df=args.max_df,
                    max_features=args.max_features,
                    sublinear_tf=True,
                    norm="l2",
                    token_pattern=r"(?u)\b(?=[a-z0-9']*[a-z])[a-z0-9']{2,}\b",
                ),
            ),
            (
                "classifier",
                LogisticRegression(
                    penalty="l2",
                    C=args.c,
                    solver="lbfgs",
                    max_iter=2000,
                    random_state=args.random_state,
                ),
            ),
        ]
    )

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    labels = ["negative", "positive"]
    report = {
        "input_file": str(args.input),
        "text_column": args.text_column,
        "label_column": args.label_column,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "test_size": args.test_size,
        "random_state": args.random_state,
        "vectorizer": {
            "min_df": args.min_df,
            "max_df": args.max_df,
            "max_features": args.max_features,
            "ngram_range": [1, 2],
        },
        "classifier": {
            "model": "LogisticRegression",
            "penalty": "l2",
            "solver": "lbfgs",
            "C": args.c,
        },
        "metrics": {
            "accuracy": accuracy_score(y_test, y_pred),
            "macro_f1": f1_score(y_test, y_pred, average="macro"),
            "weighted_f1": f1_score(y_test, y_pred, average="weighted"),
        },
        "classification_report": classification_report(y_test, y_pred, output_dict=True),
        "confusion_matrix": {
            "labels": labels,
            "matrix": confusion_matrix(y_test, y_pred, labels=labels).tolist(),
        },
        "label_distribution_full": dataset[args.label_column].value_counts().to_dict(),
        "label_distribution_train": y_train.value_counts().to_dict(),
        "label_distribution_test": y_test.value_counts().to_dict(),
    }

    args.report_output.write_text(json.dumps(report, indent=2))
    dump(pipeline, args.model_output)

    test_rows = dataset.loc[test_idx].copy()
    test_rows["predicted_label"] = y_pred
    if hasattr(pipeline.named_steps["classifier"], "predict_proba"):
        probabilities = pipeline.predict_proba(X_test)
        class_names = list(pipeline.named_steps["classifier"].classes_)
        for idx, class_name in enumerate(class_names):
            test_rows[f"proba_{class_name}"] = probabilities[:, idx]
    test_rows.to_csv(args.predictions_output, index=False)

    print(f"Training rows: {len(X_train)}")
    print(f"Test rows: {len(X_test)}")
    print(f"Accuracy: {report['metrics']['accuracy']:.4f}")
    print(f"Macro F1: {report['metrics']['macro_f1']:.4f}")
    print(f"Weighted F1: {report['metrics']['weighted_f1']:.4f}")
    print(f"Saved model to: {args.model_output}")
    print(f"Saved report to: {args.report_output}")
    print(f"Saved test predictions to: {args.predictions_output}")


if __name__ == "__main__":
    main()
