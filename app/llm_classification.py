from __future__ import annotations
import json
import os
import time
import logging
import requests
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# --------- network settings (tune for your infra) ----------
LLM_API_URL = "https://api.perplexity.ai/chat/completions"
LLM_MODEL = "sonar"
REQUEST_TIMEOUT = 30            # seconds
MAX_RETRIES = 4
BACKOFF_BASE = 0.75             # seconds, exponential backoff base
RATE_LIMIT_QPS = 1.3            # soft rate limit for bulk jobs

# lightweight token bucket
_last_ts = 0.0
def _rate_limit():
    global _last_ts
    if RATE_LIMIT_QPS <= 0:
        return
    min_interval = 1.0 / RATE_LIMIT_QPS
    now = time.monotonic()
    wait = min_interval - (now - _last_ts)
    if wait > 0:
        time.sleep(wait)
    _last_ts = time.monotonic()

def _extract_json(text: str) -> Optional[dict]:
    """
    Robustly pull a JSON object from a model response that might include prose or code fences.
    """
    if not text:
        return None

    # Strip code fences if present
    if "```" in text:
        # take the largest JSON-looking chunk between fences
        chunks = text.split("```")
        for ch in chunks:
            ch = ch.strip()
            if ch.startswith("{") and ch.endswith("}"):
                try:
                    return json.loads(ch)
                except Exception:
                    pass

    # Fallback: grab from first '{' to the matching last '}'
    try:
        first = text.find("{")
        last = text.rfind("}")
        if first != -1 and last != -1 and last > first:
            candidate = text[first:last+1]
            return json.loads(candidate)
    except Exception:
        pass

    # Final attempt: direct parse
    try:
        return json.loads(text)
    except Exception:
        return None

def _norm(s: Optional[str]) -> str:
    import re
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _clip01(x: float) -> float:
    try:
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return 0.0

def llm_assign(
    listing,
    candidate_trims: List[str],
    *,
    min_ai_confidence: float = 0.55
) -> Dict[str, object]:
    """
    Use Perplexity to map a listing to a canonical trim.
    Returns: {trim: <string or ''>, confidence: [0,1], assignment_method: 'LLM'}
    - Validates trim against candidate list (case-insensitive).
    - Rate-limited, retried with exponential backoff.
    """

    api_key = os.environ.get("PERP_API_KEY") or os.environ.get("PERPLEXITY_API_KEY")
    if not api_key:
        logger.warning("PERP_API_KEY not set; skipping LLM.")
        return {"trim": "", "confidence": 0.0, "assignment_method": "unmatched"}

    # Build prompt safely (truncate long fields)
    title = (getattr(listing, "title", None) or "")[:180]
    desc = (getattr(listing, "description", None) or "")[:1000]  # keep prompt bounded
    brand = getattr(listing, "brand", "") or ""
    model = getattr(listing, "model", "") or ""
    raw_trim = getattr(listing, "trim", "") or ""

    # Numbered list for clarity
    lines = []
    for i, t in enumerate(candidate_trims[:300], start=1):  # hard cap to keep token usage sane
        lines.append(f"{i}. {t}")

    prompt = f"""
You are an expert automotive analyst specializing in GCC market vehicle trim identification. Map the raw trim to the best canonical trim from the list. Choose ONLY from the list; do not invent names.

LISTING:
- Make: {brand}
- Model: {model}
- Title: {title}
- Raw Trim: "{raw_trim}"
- Description: {desc}

CANDIDATE TRIMS:
{chr(10).join(lines)}

RULES:
1) Pick exactly one from the list above (or empty if no acceptable match).
2) Prefer precise matches (engine, drivetrain, edition) over superficial keywords.
3) If uncertain between close options, choose the more common/base trim.
4) If confidence < 0.40, return empty trim.
5) Never return "Other/Unknown/Generic".

RESPONSE (STRICT JSON):
{{
  "trim": "<exact candidate from list or empty string>",
  "confidence": <0.0 to 1.0>,
  "assignment_method": "LLM"
}}
""".strip()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 250,
        "temperature": 0.2,
        "top_p": 0.9,
    }

    # Prepare a case-insensitive validator
    norm_map = { _norm(t): t for t in candidate_trims if t }
    norm_keys = set(norm_map.keys())

    # Retry loop with backoff
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _rate_limit()
            resp = requests.post(
                LLM_API_URL,
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code >= 500:
                raise requests.HTTPError(f"Server {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            parsed = _extract_json(content)

            if not parsed:
                raise ValueError("LLM response did not contain valid JSON.")

            raw_trim_out = parsed.get("trim") or ""
            conf_out = _clip01(parsed.get("confidence", 0.0))

            # Validate & normalize the model's choice
            if raw_trim_out:
                chosen_norm = _norm(raw_trim_out)
                if chosen_norm in norm_keys and conf_out >= min_ai_confidence:
                    return {
                        "trim": norm_map[chosen_norm],
                        "confidence": conf_out,
                        "assignment_method": "LLM",
                    }
                else:
                    # Either not in candidates or too low confidence
                    return {"trim": "", "confidence": 0.0, "assignment_method": "unmatched"}
            else:
                return {"trim": "", "confidence": 0.0, "assignment_method": "unmatched"}

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as e:
            last_exc = e
            sleep_s = BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning("LLM call failed (attempt %d/%d): %s; backing off %.2fs",
                           attempt, MAX_RETRIES, e, sleep_s)
            time.sleep(sleep_s)
        except Exception as e:
            # Unexpected; don't keep retrying forever
            logger.exception("Unexpected LLM error: %s", e)
            return {"trim": "", "confidence": 0.0, "assignment_method": "unmatched"}

    logger.error("LLM failed after %d attempts: %s", MAX_RETRIES, last_exc)
    return {"trim": "", "confidence": 0.0, "assignment_method": "unmatched"}
