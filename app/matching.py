from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Dict, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import text
from rapidfuzz import process, fuzz
import logging
import re

from .llm_classification import llm_assign

logger = logging.getLogger(__name__)

# ---------------------------
# Helper utilities
# ---------------------------

_WS_RE = re.compile(r"\s+")
_ALNUM_RE = re.compile(r"[^a-z0-9]+")

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.lower()
    s = _ALNUM_RE.sub(" ", s)      # keep word boundaries
    s = _WS_RE.sub(" ", s).strip()
    return s

def _clip01(x: float) -> float:
    try:
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return 0.0

def _canonical_method(name: Optional[str]) -> str:
    """
    Map any legacy/variant names into a small canonical set.
    """
    if not name:
        return "unmatched"
    n = name.strip().lower()
    if n in {"exact"}:
        return "exact"
    if "manual" in n:
        return "manual"
    if "llm" in n or "ai" in n:
        return "LLM"
    if "fuzzy" in n:
        return "fuzzy"
    if "rule" in n or "mapping" in n or "heuristic" in n or "canonical" in n or "closest" in n or "string" in n:
        # Fold all old rule-based labels into fuzzy unless you keep a separate bucket.
        return "fuzzy"
    if n == "unmatched" or n == "null":
        return "unmatched"
    # Default fallback
    return "fuzzy"

@dataclass
class ListingInput:
    brand: str
    model: str
    trim: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_obj(cls, listing) -> "ListingInput":
        # tolerate plain objects with attributes
        return cls(
            brand=getattr(listing, "brand", "") or "",
            model=getattr(listing, "model", "") or "",
            trim=getattr(listing, "trim", None),
            title=getattr(listing, "title", None),
            description=getattr(listing, "description", None),
        )

# ---------------------------
# Candidate trims
# ---------------------------

def get_candidate_trims(db: Session, brand: str, model: str, limit: int = 2000) -> List[str]:
    """
    Get all possible trims for a brand/model from trim_master + aliases.
    Deduplicates and returns original strings (not normalized).
    """
    rows = db.execute(
    text("""
        SELECT trim_name
        FROM trim_master
        WHERE REGEXP_REPLACE(LOWER(make), '[^a-z0-9]', '', 'g')
              = REGEXP_REPLACE(LOWER(:brand), '[^a-z0-9]', '', 'g')
          AND REGEXP_REPLACE(LOWER(model), '[^a-z0-9]', '', 'g')
              = REGEXP_REPLACE(LOWER(:model), '[^a-z0-9]', '', 'g')
        UNION
        SELECT alias
        FROM trim_alias ta
        JOIN trim_master tm ON ta.trim_master_id = tm.id
        WHERE REGEXP_REPLACE(LOWER(tm.make), '[^a-z0-9]', '', 'g')
              = REGEXP_REPLACE(LOWER(:brand), '[^a-z0-9]', '', 'g')
          AND REGEXP_REPLACE(LOWER(tm.model), '[^a-z0-9]', '', 'g')
              = REGEXP_REPLACE(LOWER(:model), '[^a-z0-9]', '', 'g')
        LIMIT :limit
    """),
    {"brand": brand, "model": model, "limit": limit}
).fetchall()


    # Deduplicate while preserving input order
    seen = set()
    out: List[str] = []
    for r in rows:
        t = (r[0] or "").strip()
        if not t:
            continue
        if t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    return out

# ---------------------------
# Matching core
# ---------------------------

def match_trim(
    listing,
    candidate_trims: Iterable[str],
    *,
    fuzzy_primary_threshold: int = 82,       # robust token_set primary threshold
    fuzzy_secondary_threshold: int = 74,     # allow slightly lower with combined fields
    min_ai_confidence: float = 0.55,
    allow_external_llm: bool = True
) -> Dict[str, object]:
    """
    Robust matching pipeline:
      1) Exact (normalized)
      2) Fuzzy on trim
      3) Fuzzy on title/description + trim combined (token_set/partial mix)
      4) LLM (optional)
      5) Unmatched
    Returns: {trim, confidence, assignment_method}
    """
    li = ListingInput.from_obj(listing)

    # Prepare candidate maps
    cand_list = [c for c in (candidate_trims or []) if c]
    if not cand_list:
        return {"trim": None, "confidence": 0.0, "assignment_method": "unmatched"}

    norm_to_original: Dict[str, str] = {}
    normalized_candidates: List[str] = []
    for t in cand_list:
        n = _norm(t)
        if n and n not in norm_to_original:
            norm_to_original[n] = t
            normalized_candidates.append(n)

    raw_trim_norm = _norm(li.trim)

    # 1) Exact (normalized)
    if raw_trim_norm and raw_trim_norm in norm_to_original:
        return {
            "trim": norm_to_original[raw_trim_norm],
            "confidence": 1.0,
            "assignment_method": "exact",
        }

    if allow_external_llm:
        try:
            ai_guess = llm_assign(li, cand_list, min_ai_confidence=min_ai_confidence)
            ai_trim = ai_guess.get("trim") or ""
            ai_conf = _clip01(ai_guess.get("confidence", 0.0))
            ai_method = _canonical_method(ai_guess.get("assignment_method", "LLM"))

            # Only accept if the chosen trim is actually in candidate list (case-insensitive)
            print(f"AI inferred: {ai_trim}")
            if ai_trim:
                chosen_norm = _norm(ai_trim)
                if chosen_norm in norm_to_original and ai_conf >= min_ai_confidence:
                    return {
                        "trim": norm_to_original[chosen_norm],
                        "confidence": ai_conf,
                        "assignment_method": ai_method,
                    }
        except Exception as e:
            logger.exception("LLM assignment failed: %s", e)


    # Helper to choose best fuzzy candidate by scorer
    def _best_by_scorer(query: str, scorer, candidates: List[str]) -> Tuple[Optional[str], int]:
        if not query:
            return None, 0
        res = process.extractOne(query, candidates, scorer=scorer)  # (match, score, idx)
        if not res:
            return None, 0
        return res[0], int(res[1])

    # 2) Fuzzy on the trim alone (token_set_ratio is generally robust to word order)
    best_val, best_score = _best_by_scorer(raw_trim_norm, fuzz.token_set_ratio, normalized_candidates)
    if best_val and best_score >= fuzzy_primary_threshold:
        return {
            "trim": norm_to_original[best_val],
            "confidence": _clip01(best_score / 100.0),
            "assignment_method": "fuzzy",
        }

    # 3) Fuzzy using combined evidence (trim + title + description)
    # Build a combined query to capture extra clues
    title_norm = _norm(li.title)
    desc_norm = _norm(li.description)
    # use only first ~300 chars of description-equivalent tokens to keep fast
    if len(desc_norm) > 300:
        desc_norm = desc_norm[:300]

    combined_queries = [q for q in [raw_trim_norm, title_norm] if q]
    combined = " ".join(combined_queries).strip()
    # try multiple scorers and take the best
    candidates = normalized_candidates

    alt_candidates = []
    alt_scores = []

    if combined:
        v1, s1 = _best_by_scorer(combined, fuzz.token_set_ratio, candidates)
        if v1: alt_candidates.append(v1); alt_scores.append(s1)

        v2, s2 = _best_by_scorer(combined, fuzz.partial_ratio, candidates)
        if v2: alt_candidates.append(v2); alt_scores.append(s2)

    if not combined and title_norm:
        combined = title_norm

    # If nothing yet, try combined + short description
    if not alt_candidates and (combined or desc_norm):
        combo2 = (combined + " " + desc_norm).strip()
        v3, s3 = _best_by_scorer(combo2, fuzz.token_set_ratio, candidates)
        if v3: alt_candidates.append(v3); alt_scores.append(s3)

    if alt_candidates:
        idx = max(range(len(alt_scores)), key=lambda i: alt_scores[i])
        cand_norm = alt_candidates[idx]
        cand_score = alt_scores[idx]
        if cand_score >= fuzzy_secondary_threshold:
            return {
                "trim": norm_to_original[cand_norm],
                "confidence": _clip01(cand_score / 100.0),
                "assignment_method": "fuzzy",
            }

    # 4) LLM (optional and only if we still don't have a solid fuzzy)

    # 5) No match
    return {"trim": None, "confidence": 0.0, "assignment_method": "unmatched"}
