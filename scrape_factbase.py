import asyncio
import re
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright


OUTPUT_FILE = Path("factbase_truthsocial_texts.csv")

# normal wait between scrolls
SCROLL_PAUSE_SECONDS = 3.5

# longer wait when a round found nothing new
STALL_WAIT_SECONDS = 6.0

# let it tolerate more temporary "nothing new" rounds
MAX_STAGNANT_ROUNDS = 12

START_URL = "https://factba.se/"

# after you manually open the correct archive page and press Enter,
# the script closes the visible browser and resumes scraping headlessly
# from that page URL so it can run in the background.
RUN_HEADLESS_AFTER_SELECTION = True


DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December) "
    r"\d{1,2}, \d{4} @ \d{1,2}:\d{2} (AM|PM) ET"
)

ENGAGEMENT_RE = re.compile(
    r"\b\d[\d,]*\s+(ReTruths?|Likes?|Replies?|Reposts?)\b",
    re.IGNORECASE,
)


def extract_date_et(raw_text: str) -> str:
    if not raw_text:
        return ""
    m = DATE_RE.search(raw_text)
    return m.group(0) if m else ""


def clean_post_text(raw_text: str) -> str:
    if not raw_text:
        return ""

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
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
    text = DATE_RE.sub("", text)
    text = re.sub(r"\bView on Truth Social\b", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\b\d[\d,]*\s+(ReTruths?|Likes?|Replies?|Reposts?)\b",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text).strip()

    return text


def load_existing_rows() -> list[dict]:
    if not OUTPUT_FILE.exists():
        return []

    df = pd.read_csv(OUTPUT_FILE)
    needed = ["date_et", "truth_social_url", "text", "raw_card_text"]
    for col in needed:
        if col not in df.columns:
            df[col] = ""
    return df[needed].fillna("").to_dict(orient="records")


def save_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        df = pd.DataFrame(columns=["date_et", "truth_social_url", "text", "raw_card_text"])
    else:
        df = pd.DataFrame(rows)
        for col in ["date_et", "truth_social_url", "text", "raw_card_text"]:
            if col not in df.columns:
                df[col] = ""
        df = df[["date_et", "truth_social_url", "text", "raw_card_text"]]

        with_url = df[df["truth_social_url"].astype(str).str.strip() != ""].drop_duplicates(
            subset=["truth_social_url"], keep="first"
        )
        no_url = df[df["truth_social_url"].astype(str).str.strip() == ""].copy()

        if not no_url.empty:
            no_url["_fallback_key"] = no_url["raw_card_text"].astype(str).str[:300]
            no_url = no_url.drop_duplicates(subset=["_fallback_key"], keep="first").drop(
                columns=["_fallback_key"]
            )

        df = pd.concat([with_url, no_url], ignore_index=True)

    df.to_csv(OUTPUT_FILE, index=False)
    return df


async def get_selected_page(context):
    pages = [p for p in context.pages if not p.is_closed()]
    if not pages:
        return None

    non_blank = [p for p in pages if p.url and p.url != "about:blank"]
    return non_blank[-1] if non_blank else pages[-1]


async def open_start_page(page):
    await page.goto(START_URL, wait_until="domcontentloaded")
    await asyncio.sleep(2)


async def debug_page_info(page, label=""):
    try:
        prefix = f"{label} " if label else ""
        print(f"{prefix}Current page URL: {page.url}")
        print(f"{prefix}Current page title: {await page.title()}")
    except Exception as e:
        print(f"Could not read page info: {e}")


async def launch_browser(p, headless: bool):
    if headless:
        # Prefer Chromium channel for modern headless mode; fall back if unavailable.
        try:
            browser = await p.chromium.launch(channel="chromium", headless=True)
        except Exception:
            browser = await p.chromium.launch(headless=True)
    else:
        try:
            browser = await p.chromium.launch(channel="chrome", headless=False, slow_mo=150)
        except Exception:
            browser = await p.chromium.launch(headless=False, slow_mo=150)

    context = await browser.new_context(viewport={"width": 1440, "height": 2200})
    page = await context.new_page()
    return browser, context, page


async def collect_visible_cards(page) -> list[dict]:
    js = r"""
    () => {
        function isVisible(el) {
            if (!el) return false;
            const style = window.getComputedStyle(el);
            if (!style) return false;
            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
        }

        function cleanText(s) {
            return (s || '')
                .replace(/\u00A0/g, ' ')
                .replace(/[ \t]+\n/g, '\n')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
        }

        function findUrlInContainer(el) {
            if (!el) return '';
            const anchors = Array.from(el.querySelectorAll('a[href]'));

            for (const a of anchors) {
                const href = a.href || '';
                if (href.includes('truthsocial.com') && href.includes('/posts/')) {
                    return href;
                }
            }

            for (const a of anchors) {
                const href = a.href || '';
                if (href.includes('/posts/')) {
                    return href;
                }
            }

            return '';
        }

        function chooseBestContainer(seed) {
            let el = seed;
            let best = null;

            for (let i = 0; el && i < 12; i++, el = el.parentElement) {
                if (!isVisible(el)) continue;

                const txt = cleanText(el.innerText || '');
                if (!txt) continue;
                if (txt.length < 40) continue;
                if (txt.length > 7000) continue;

                const looksLikeCard =
                    txt.includes('View on Truth Social') ||
                    txt.includes('@realDonaldTrump') ||
                    txt.includes('Donald Trump');

                if (looksLikeCard) {
                    best = el;
                }

                if (
                    txt.includes('View on Truth Social') &&
                    (txt.includes('@realDonaldTrump') || txt.includes('Donald Trump'))
                ) {
                    return el;
                }
            }

            return best;
        }

        const all = Array.from(document.querySelectorAll('*'));
        const seeds = all.filter(el => {
            if (!isVisible(el)) return false;
            const txt = cleanText(el.innerText || '');
            if (!txt) return false;

            return (
                txt.includes('View on Truth Social') &&
                (txt.includes('@realDonaldTrump') || txt.includes('Donald Trump'))
            );
        });

        const out = [];
        const seen = new Set();

        for (const seed of seeds) {
            const card = chooseBestContainer(seed);
            if (!card) continue;

            const raw = cleanText(card.innerText || '');
            if (!raw) continue;

            const url = findUrlInContainer(card);
            const key = url || raw.slice(0, 300);

            if (seen.has(key)) continue;
            seen.add(key);

            out.push({
                truth_social_url: url,
                raw_card_text: raw
            });
        }

        return out;
    }
    """
    return await page.evaluate(js)


async def main():
    rows = load_existing_rows()

    seen_urls = {
        row.get("truth_social_url", "").strip()
        for row in rows
        if row.get("truth_social_url", "").strip()
    }

    seen_fallback_keys = {
        row.get("raw_card_text", "")[:300]
        for row in rows
        if row.get("raw_card_text", "")
    }

    async with async_playwright() as p:
        # Step 1: visible browser so you can manually get to the right page
        browser, context, page = await launch_browser(p, headless=False)
        await open_start_page(page)

        print("\nChrome opened Factba.se directly.")
        print("Now manually navigate to the Trump Truth Social archive page.")
        print("Use the same window/tab you want the script to continue with.")
        print("Wait until posts are visible.\n")
        input("Press Enter only after the correct archive page is fully open and loaded... ")

        selected_page = await get_selected_page(context)
        if selected_page is None:
            print("No selected page found.")
            await browser.close()
            return

        page = selected_page
        await asyncio.sleep(2)
        await debug_page_info(page, label="[VISIBLE]")

        # lock in the URL once you've chosen the correct page
        selected_url = page.url

        # Optional: switch to headless mode so it runs in the background
        if RUN_HEADLESS_AFTER_SELECTION:
            print("\nSwitching to headless background scraping...")
            await browser.close()

            browser, context, page = await launch_browser(p, headless=True)
            await page.goto(selected_url, wait_until="domcontentloaded")
            await asyncio.sleep(4)
            await debug_page_info(page, label="[HEADLESS]")

        stagnant_rounds = 0

        while True:
            if page.is_closed():
                print("Page was closed. Stopping.")
                break

            visible_cards = await collect_visible_cards(page)
            before_total = len(seen_urls) + len(seen_fallback_keys)
            new_rows = 0

            for item in visible_cards:
                url = (item.get("truth_social_url") or "").strip()
                raw = (item.get("raw_card_text") or "").strip()

                if not raw:
                    continue

                fallback_key = raw[:300]

                if url:
                    if url in seen_urls:
                        continue
                else:
                    if fallback_key in seen_fallback_keys:
                        continue

                row = {
                    "date_et": extract_date_et(raw),
                    "truth_social_url": url,
                    "text": clean_post_text(raw),
                    "raw_card_text": raw,
                }

                rows.append(row)
                new_rows += 1

                if url:
                    seen_urls.add(url)
                else:
                    seen_fallback_keys.add(fallback_key)

            df = save_rows(rows)

            print(
                f"Visible cards parsed: {len(visible_cards)} | "
                f"new rows added this round: {new_rows} | "
                f"total saved so far: {len(df)} | "
                f"stagnant rounds: {stagnant_rounds}"
            )

            if len(visible_cards) == 0:
                try:
                    snippet = await page.locator("body").inner_text(timeout=3000)
                    print("\nBody text preview:")
                    print(snippet[:1000])
                    print()
                except Exception:
                    pass

            after_total = len(seen_urls) + len(seen_fallback_keys)

            if after_total == before_total and new_rows == 0:
                stagnant_rounds += 1
                await page.mouse.wheel(0, 12000)
                await asyncio.sleep(STALL_WAIT_SECONDS)

                # one more quick check after extra waiting before giving up
                retry_cards = await collect_visible_cards(page)
                retry_new = 0

                for item in retry_cards:
                    url = (item.get("truth_social_url") or "").strip()
                    raw = (item.get("raw_card_text") or "").strip()

                    if not raw:
                        continue

                    fallback_key = raw[:300]

                    if url:
                        if url in seen_urls:
                            continue
                    else:
                        if fallback_key in seen_fallback_keys:
                            continue

                    row = {
                        "date_et": extract_date_et(raw),
                        "truth_social_url": url,
                        "text": clean_post_text(raw),
                        "raw_card_text": raw,
                    }

                    rows.append(row)
                    retry_new += 1

                    if url:
                        seen_urls.add(url)
                    else:
                        seen_fallback_keys.add(fallback_key)

                if retry_new > 0:
                    df = save_rows(rows)
                    stagnant_rounds = 0
                    print(
                        f"Retry after stall found {retry_new} more rows | "
                        f"total saved so far: {len(df)}"
                    )
                elif stagnant_rounds >= MAX_STAGNANT_ROUNDS:
                    print("No new posts found after many retries. Stopping.")
                    break
            else:
                stagnant_rounds = 0
                await page.mouse.wheel(0, 12000)
                await asyncio.sleep(SCROLL_PAUSE_SECONDS)

        await browser.close()

    final_df = pd.read_csv(OUTPUT_FILE) if OUTPUT_FILE.exists() else pd.DataFrame()
    print(f"\nDone. Saved {len(final_df)} rows to {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())