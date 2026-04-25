from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer


DEFAULT_INPUT = Path("factbase_truthsocial_texts_nlp_ready.csv")
DEFAULT_OUTPUT = Path("factbase_truthsocial_texts_sentiment_labeled.csv")
DEFAULT_SUMMARY = Path("factbase_truthsocial_texts_sentiment_label_summary.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto-label Truth Social posts with bootstrap sentiment labels."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--summary-output", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--text-column", default="text_clean")
    return parser.parse_args()


def sentiment_label(compound: float) -> str:
    return "positive" if compound >= 0 else "negative"


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.input)
    if args.text_column not in df.columns:
        raise ValueError(f"Column '{args.text_column}' was not found in {args.input}.")

    sia = SentimentIntensityAnalyzer()
    scores = df[args.text_column].fillna("").astype(str).map(sia.polarity_scores)
    score_frame = pd.DataFrame(list(scores))

    labeled = df.copy()
    labeled["sentiment_neg"] = score_frame["neg"]
    labeled["sentiment_neu"] = score_frame["neu"]
    labeled["sentiment_pos"] = score_frame["pos"]
    labeled["sentiment_compound"] = score_frame["compound"]
    labeled["sentiment_label"] = labeled["sentiment_compound"].map(sentiment_label)

    labeled.to_csv(args.output, index=False)

    summary = {
        "input_file": str(args.input),
        "output_file": str(args.output),
        "text_column": args.text_column,
        "row_count": int(labeled.shape[0]),
        "label_counts": labeled["sentiment_label"].value_counts().to_dict(),
        "target_labels": ["negative", "positive"],
    }
    args.summary_output.write_text(json.dumps(summary, indent=2))

    print(f"Labeled rows: {labeled.shape[0]}")
    print("Label distribution:")
    print(labeled["sentiment_label"].value_counts().to_string())
    print(f"Saved labeled dataset to: {args.output}")
    print(f"Saved summary to: {args.summary_output}")


if __name__ == "__main__":
    main()
