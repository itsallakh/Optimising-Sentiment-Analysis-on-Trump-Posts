# Optimising-Sentiment-Analysis-on-Trump-Posts

Clean the scraped Truth Social dataset with:

```bash
python prepare_dataset.py
```

That creates `factbase_truthsocial_texts_clean.csv` with:

- `text_clean`: post text with the repeated Truth Social header removed
- `date_parsed_et`: parsed ET timestamp ready for sorting, filtering, and feature work
- `post_id`: extracted from the Truth Social URL
- `text_length`, `word_count`, `is_duplicate_text`: quick analysis features

Example dataframe workflow:

```python
from prepare_dataset import load_clean_dataframe

df = load_clean_dataframe()
print(df[["date_parsed_et", "text_clean"]].head())
```
