from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
from joblib import dump
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer


DEFAULT_INPUT = Path("factbase_truthsocial_texts_clean.csv")
DEFAULT_NLP_READY = Path("factbase_truthsocial_texts_nlp_ready.csv")
DEFAULT_TFIDF_MATRIX = Path("factbase_truthsocial_texts_tfidf.npz")
DEFAULT_FEATURES = Path("factbase_truthsocial_texts_tfidf_features.csv")
DEFAULT_VECTORIZER = Path("factbase_truthsocial_tfidf_vectorizer.joblib")
DEFAULT_CONFIG = Path("factbase_truthsocial_tfidf_config.json")

WHITESPACE_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
MENTION_RE = re.compile(r"@\w+")
NON_WORD_RE = re.compile(r"[^a-z0-9_'\s]")
UNDERSCORE_RUN_RE = re.compile(r"_+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare Trump Truth Social posts for NLP and TF-IDF modeling."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--nlp-ready-output", type=Path, default=DEFAULT_NLP_READY)
    parser.add_argument("--tfidf-output", type=Path, default=DEFAULT_TFIDF_MATRIX)
    parser.add_argument("--features-output", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--vectorizer-output", type=Path, default=DEFAULT_VECTORIZER)
    parser.add_argument("--config-output", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--text-column", default="text_clean")
    parser.add_argument("--min-df", type=int, default=2)
    parser.add_argument("--max-df", type=float, default=0.98)
    parser.add_argument("--max-features", type=int, default=5000)
    parser.add_argument("--drop-duplicate-text", action="store_true")
    return parser.parse_args()


def normalize_for_nlp(text: str) -> str:
    text = "" if pd.isna(text) else str(text)
    text = text.lower()
    text = URL_RE.sub(" url ", text)
    text = MENTION_RE.sub(" user ", text)
    text = text.replace("&amp;", " and ")
    text = NON_WORD_RE.sub(" ", text)
    text = UNDERSCORE_RUN_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def build_nlp_ready_frame(df: pd.DataFrame, text_column: str) -> pd.DataFrame:
    if text_column not in df.columns:
        raise ValueError(f"Column '{text_column}' was not found in the input file.")

    working = df.copy()
    working["text_for_nlp"] = working[text_column].map(normalize_for_nlp)
    working = working[working["text_for_nlp"].str.len() > 0].copy()

    if "is_duplicate_text" not in working.columns:
        working["is_duplicate_text"] = working["text_for_nlp"].duplicated(keep="first")

    working["sentiment_label"] = ""

    preferred_columns = [
        "post_id",
        "date_parsed_et",
        "date_et",
        "truth_social_url",
        text_column,
        "text_for_nlp",
        "text_length",
        "word_count",
        "is_duplicate_text",
        "sentiment_label",
    ]
    existing_columns = [column for column in preferred_columns if column in working.columns]
    extra_columns = [column for column in working.columns if column not in existing_columns]
    return working[existing_columns + extra_columns]


def save_config(args: argparse.Namespace, n_rows: int, n_features: int) -> None:
    config = {
        "input_file": str(args.input),
        "text_column": args.text_column,
        "min_df": args.min_df,
        "max_df": args.max_df,
        "max_features": args.max_features,
        "drop_duplicate_text": args.drop_duplicate_text,
        "n_documents": n_rows,
        "n_features": n_features,
        "recommended_model": "LogisticRegression(penalty='l2')",
        "target_labels": ["negative", "positive"],
    }
    args.config_output.write_text(json.dumps(config, indent=2))


def main() -> None:
    args = parse_args()
    source = pd.read_csv(args.input)
    nlp_ready = build_nlp_ready_frame(source, args.text_column)

    if args.drop_duplicate_text:
        nlp_ready = nlp_ready.drop_duplicates(subset=["text_for_nlp"], keep="first").copy()

    vectorizer = TfidfVectorizer(
        lowercase=False,
        strip_accents="unicode",
        ngram_range=(1, 2),
        min_df=args.min_df,
        max_df=args.max_df,
        max_features=args.max_features,
        sublinear_tf=True,
        norm="l2",
        # Keep tokens that contain at least one letter and have length >= 2.
        token_pattern=r"(?u)\b(?=[a-z0-9']*[a-z])[a-z0-9']{2,}\b",
    )

    matrix = vectorizer.fit_transform(nlp_ready["text_for_nlp"])
    feature_names = vectorizer.get_feature_names_out()

    nlp_ready.to_csv(args.nlp_ready_output, index=False)
    sparse.save_npz(args.tfidf_output, matrix)
    pd.DataFrame({"feature_name": feature_names}).to_csv(args.features_output, index=False)
    dump(vectorizer, args.vectorizer_output)
    save_config(args, n_rows=nlp_ready.shape[0], n_features=len(feature_names))

    print(f"NLP-ready rows: {nlp_ready.shape[0]}")
    print(f"TF-IDF matrix shape: {matrix.shape[0]} rows x {matrix.shape[1]} features")
    print(f"Saved NLP-ready CSV to: {args.nlp_ready_output}")
    print(f"Saved sparse TF-IDF matrix to: {args.tfidf_output}")
    print(f"Saved feature names to: {args.features_output}")
    print(f"Saved vectorizer to: {args.vectorizer_output}")
    print(f"Saved config to: {args.config_output}")


if __name__ == "__main__":
    main()
