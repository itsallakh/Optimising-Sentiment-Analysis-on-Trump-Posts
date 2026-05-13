from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/raw/factbase_truthsocial_texts.csv")
DEFAULT_OUTPUT = Path("data/processed/factbase_truthsocial_texts_clean.csv")
POST_ID_RE = re.compile(r"/posts/(\d+)")
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December) "
    r"\d{1,2}, \d{4} @ \d{1,2}:\d{2} (AM|PM) ET"
)
ENGAGEMENT_RE = re.compile(
    r"\b\d[\d,]*\s+(ReTruths?|Likes?|Replies?|Reposts?)\b",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean scraped Factbase Truth Social post data.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def extract_post_id(url: str) -> str:
    match = POST_ID_RE.search("" if pd.isna(url) else str(url))
    return match.group(1) if match else ""


def parse_date_et(value: str) -> str:
    parsed = pd.to_datetime(
        "" if pd.isna(value) else str(value),
        format="%B %d, %Y @ %I:%M %p ET",
        errors="coerce",
    )
    return "" if pd.isna(parsed) else parsed.strftime("%Y-%m-%d %H:%M:%S")


def clean_post_text(raw_text: str) -> str:
    if not raw_text or pd.isna(raw_text):
        return ""

    lines = [line.strip() for line in str(raw_text).splitlines() if line.strip()]
    cleaned_lines = []
    skip_exact = {
        "Donald Trump",
        "@realDonaldTrump",
        "Truth Social",
        "View on Truth Social",
    }

    for line in lines:
        if line in skip_exact:
            continue
        if DATE_RE.fullmatch(line):
            continue
        if ENGAGEMENT_RE.fullmatch(line):
            continue
        if "Donald Trump @realDonaldTrump" in line and "Truth Social" in line:
            continue
        if line.startswith("@realDonaldTrump") and "Truth Social" in line:
            continue
        if line.startswith("Donald Trump @realDonaldTrump"):
            continue
        if "View on Truth Social" in line and len(line) < 120:
            continue
        cleaned_lines.append(line)

    text = "\n".join(cleaned_lines).strip()
    text = re.sub(
        r"Donald Trump\s*@realDonaldTrump\s*[•·]?\s*Truth Social\s*[•·]?\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"@realDonaldTrump\s*[•·]?\s*Truth Social\s*[•·]?\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = DATE_RE.sub("", text)
    text = re.sub(r"\bView on Truth Social\b", "", text, flags=re.IGNORECASE)
    text = ENGAGEMENT_RE.sub("", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text).strip()
    return text


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Raw input file was not found: {args.input}")

    df = pd.read_csv(args.input)
    for column in ["date_et", "truth_social_url", "text", "raw_card_text"]:
        if column not in df.columns:
            df[column] = ""

    cleaned = df.copy()
    cleaned["post_id"] = cleaned["truth_social_url"].map(extract_post_id)
    cleaned["date_parsed_et"] = cleaned["date_et"].map(parse_date_et)
    cleaned["text_clean"] = cleaned["text"].fillna("").astype(str).map(clean_post_text)
    fallback_empty = cleaned["text_clean"].str.len() == 0
    cleaned.loc[fallback_empty, "text_clean"] = (
        cleaned.loc[fallback_empty, "raw_card_text"].fillna("").astype(str).map(clean_post_text)
    )
    cleaned["text_length"] = cleaned["text_clean"].str.len()
    cleaned["word_count"] = cleaned["text_clean"].str.split().map(len)
    cleaned = cleaned[cleaned["text_clean"].str.len() > 0].copy()
    cleaned["is_duplicate_text"] = cleaned["text_clean"].duplicated(keep="first")

    preferred_columns = [
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
    extra_columns = [column for column in cleaned.columns if column not in preferred_columns]
    cleaned = cleaned[preferred_columns + extra_columns]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(args.output, index=False)
    print(f"Cleaned rows: {len(cleaned)}")
    print(f"Saved cleaned dataset to: {args.output}")


if __name__ == "__main__":
    main()
