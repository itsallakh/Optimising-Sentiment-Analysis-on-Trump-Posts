from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


RAW_CSV = Path("factbase_truthsocial_texts.csv")
CLEAN_CSV = Path("factbase_truthsocial_texts_clean.csv")

TEXT_PREFIX_RE = re.compile(r"^@realDonaldTrump\s+[•·]\s+Truth Social\s+[•·]\s*", re.MULTILINE)


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def extract_post_text(text: str) -> str:
    text = TEXT_PREFIX_RE.sub("", text, count=1)
    return normalize_whitespace(text)


def extract_post_id(url: str) -> str:
    return url.rstrip("/").split("/")[-1]


def load_clean_dataframe(csv_path: Path | str = RAW_CSV) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    df["date_parsed_et"] = pd.to_datetime(
        df["date_et"].str.replace(" ET", "", regex=False),
        format="%B %d, %Y @ %I:%M %p",
        errors="coerce",
    )

    df["post_id"] = df["truth_social_url"].map(extract_post_id)
    df["text_clean"] = df["text"].map(extract_post_text)
    df["text_length"] = df["text_clean"].str.len()
    df["word_count"] = df["text_clean"].str.split().str.len()
    df["is_duplicate_text"] = df.duplicated(subset=["text_clean"], keep=False)

    ordered_columns = [
        "post_id",
        "date_parsed_et",
        "date_et",
        "truth_social_url",
        "text_clean",
        "text_length",
        "word_count",
        "is_duplicate_text",
        "text",
        "raw_card_text",
    ]
    return df[ordered_columns]


def main() -> None:
    df = load_clean_dataframe(RAW_CSV)
    df.to_csv(CLEAN_CSV, index=False, encoding="utf-8")

    print(f"Saved cleaned dataset to: {CLEAN_CSV.resolve()}")
    print(f"Rows, columns: {df.shape}")
    print(f"Duplicate cleaned texts: {int(df['is_duplicate_text'].sum())}")
    print("Sample cleaned rows:")
    print(df[["date_parsed_et", "text_clean"]].head(3).to_string(index=False, max_colwidth=100))


if __name__ == "__main__":
    main()
