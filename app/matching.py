# app/matching.py
from datetime import datetime
from rapidfuzz import process, fuzz
from typing import Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text

def match_trim(listing, candidate_trims):
    """
    Try to match a listing's raw_trim to the list of candidate trims.
    Returns a dict with trim, confidence, and assignment_method.
    """
    trim = listing.trim.strip() if listing.trim else ""

    if not candidate_trims:  # Guard against empty candidate list
        return {
            "trim": None,
            "confidence": 0.0,
            "assignment_method": "unmatched"
        }

    # Try exact match first
    if trim.lower() in [t.lower() for t in candidate_trims]:
        return {
            "trim": trim,
            "confidence": 1.0,
            "assignment_method": "exact"
        }

    # Fuzzy match
    match_tuple = process.extractOne(trim, candidate_trims, scorer=fuzz.ratio)
    if match_tuple:  # Only unpack if match_tuple is not None
        match, score = match_tuple[0], match_tuple[1]
        return {
            "trim": match,
            "confidence": score / 100.0,
            "assignment_method": "fuzzy"
        }

    # No match
    return {
        "trim": None,
        "confidence": 0.0,
        "assignment_method": "unmatched"
    }


def get_candidate_trims(db: Session, brand: str, model: str):
    """
    Get all possible trims for a brand/model from trim_master + aliases.
    """
    rows = db.execute(
        text("""
            SELECT trim_name FROM trim_master
            WHERE LOWER(make) = LOWER(:brand)
              AND LOWER(model) = LOWER(:model)
            UNION
            SELECT alias FROM trim_alias ta
            JOIN trim_master tm ON ta.trim_master_id = tm.id
            WHERE LOWER(tm.make) = LOWER(:brand)
              AND LOWER(tm.model) = LOWER(:model)
        """),
        {"brand": brand, "model": model}
    ).fetchall()

    return [r[0] for r in rows]
