"""
Language utilities for hybrid content-based English detection.
"""


from typing import List, Dict, Any

# Try to import langdetect if available
try:
    from langdetect import detect_langs
    _HAVE_LANGDETECT = True
except ImportError:
    _HAVE_LANGDETECT = False
    raise RuntimeError("The 'langdetect' package is required for language detection. "
                       "Please install it using 'pip install langdetect' to enable this functionality.")





# Hybrid language detection helper
def is_probably_english(text: str, title: str = "", min_prob: float = 0.80) -> bool:
    """Return True if the combined text/title is likely English.
    Uses langdetect if available; if not installed, returns True (to avoid blocking ingestion).
    """
    if not _HAVE_LANGDETECT:
        return True
    sample = (title + "\n" + (text or "")).strip()
    if len(sample) < 40:
        # Too short for reliable detection; let it pass (GDELT filter already applied)
        return True
    try:
        # detect_langs returns a list like [en:0.99, fr:0.01]
        langs = detect_langs(sample[:5000])  # cap to avoid heavy work on huge pages
        for lp in langs:
            if lp.lang == 'en' and lp.prob >= min_prob:
                return True
        return False
    except Exception:
        # On any detection failure, fall back to permissive (don't drop)
        return True


if __name__ == "__main__":
    print(f"_HAVE_LANGDETECT={_HAVE_LANGDETECT}")


    # Tests for content-based detection (is_probably_english)
    en_title = "UK inflation falls to 2 percent in latest ONS report"
    en_text = (
        "The Office for National Statistics reported that the Consumer Prices Index "
        "fell to 2.0% year-on-year, driven by food and energy prices."
    )

    fr_title = "La croissance française dépasse les attentes"
    fr_text = (
        "Selon l'INSEE, la croissance du PIB au deuxième trimestre a été supérieure "
        "aux prévisions des économistes."
    )

    short_text = "Hello world"

    print("is_probably_english(en):", is_probably_english(en_text, en_title))
    print("is_probably_english(fr):", is_probably_english(fr_text, fr_title))
    print("is_probably_english(short):", is_probably_english(short_text))  # short path → True