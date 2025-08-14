# app/routes/listings.py
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional, List
from datetime import datetime

from app.database import SessionLocal
from app import models, db_utils, matching

router = APIRouter()

# ----------------------
# DB dependency
# ----------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ----------------------
# Process unprocessed listings
# ----------------------
@router.post("/process-listings")
def process_listings(limit: int = 500, db: Session = Depends(get_db)):
    unprocessed = db_utils.get_unprocessed_listings(db, limit)

    processed_count = 0
    exact_matches = 0
    fuzzy_matches = 0
    unmatched = 0

    for listing in unprocessed:
        ad_id, brand, model, year, trim = listing
        candidate_trims = matching.get_candidate_trims(db, brand, model)

        listing_obj = type("Obj", (object,), {
            "raw_trim": trim,
            "brand": brand,
            "model": model
        })

        match_result = matching.match_trim(
            listing=listing_obj,
            candidate_trims=candidate_trims
        )

        method = match_result.get("assignment_method", "unmatched")
        if method == "exact":
            exact_matches += 1
        elif method == "fuzzy":
            fuzzy_matches += 1
        else:
            unmatched += 1

        # update listing & record history
        db_utils.update_listing_with_match(
            db=db,
            ad_id=ad_id,
            normalized_trim=match_result.get("trim"),
            confidence=match_result.get("confidence", 0.0),
            method=method
        )

        processed_count += 1

    return {
        "processed": processed_count,
        "exact_matches": exact_matches,
        "fuzzy_matches": fuzzy_matches,
        "unmatched": unmatched
    }


# ----------------------
# Get unprocessed listings
# ----------------------
@router.get("/listings/unprocessed")
def get_unprocessed_listings(limit: int = Query(100, ge=1), db: Session = Depends(get_db)):
    rows = db.query(
        models.Listings.ad_id,
        models.Listings.brand,
        models.Listings.model,
        models.Listings.year,
        models.Listings.trim
    ).filter(
        (models.Listings.processed_at.is_(None)) |
        (models.Listings.needs_review.is_(True))
    ).limit(limit).all()
    return [
        {"ad_id": r.ad_id, "brand": r.brand, "model": r.model, "year": r.year, "trim": r.trim}
        for r in rows
    ]


# ----------------------
# Get processed listings
# ----------------------
@router.get("/listings/processed")
def get_processed_listings(
    brand: Optional[str] = Query(None),
    model_name: Optional[str] = Query(None, alias="model"),
    method: Optional[str] = Query(None, alias="assignment_method"),
    min_conf: float = Query(0.0, ge=0.0, le=1.0),
    max_conf: float = Query(1.0, ge=0.0, le=1.0),
    limit: int = Query(100, ge=1),
    db: Session = Depends(get_db)
):
    q = db.query(models.Listings).filter(models.Listings.processed_at.isnot(None))
    if brand:
        q = q.filter(models.Listings.brand.ilike(brand))
    if model_name:
        q = q.filter(models.Listings.model.ilike(model_name))
    if method:
        q = q.filter(models.Listings.assignment_method == method)
    q = q.filter(models.Listings.trim_confidence >= min_conf, models.Listings.trim_confidence <= max_conf)

    rows = q.limit(limit).all()
    return [
        {
            "ad_id": r.ad_id,
            "brand": r.brand,
            "model": r.model,
            "year": r.year,
            "raw_trim": r.trim,
            "normalized_trim": r.normalized_trim,
            "confidence": r.trim_confidence,
            "method": r.assignment_method,
            "needs_review": r.needs_review,
            "processed_at": r.processed_at
        }
        for r in rows
    ]


# ----------------------
# Manually assign trim (with optional alias)
# ----------------------
@router.post("/listings/{ad_id}/assign-trim")
def assign_trim_manual(
    ad_id: str,
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):
    """
    payload keys:
    - trim_master_id (optional)
    - normalized_trim
    - confidence
    - changed_by
    - create_alias
    - alias_text
    """
    listing = db.query(models.Listings).filter(models.Listings.ad_id == ad_id).first()
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    trim_master_id = payload.get("trim_master_id")
    if trim_master_id:
        tm = db.query(models.TrimMaster).filter(models.TrimMaster.id == trim_master_id).first()
        if not tm:
            raise HTTPException(status_code=404, detail="Trim master not found")
        normalized_trim = tm.trim_name
    else:
        normalized_trim = payload.get("normalized_trim")

    confidence = payload.get("confidence", 1.0)
    changed_by = payload.get("changed_by", "manual")

    # Record old value before updating
    old_trim = listing.normalized_trim

    listing.normalized_trim = normalized_trim
    listing.trim_confidence = confidence
    listing.assignment_method = "manual"
    listing.needs_review = False
    listing.last_reviewed_at = datetime.utcnow()

    # Record history
    history = models.TrimHistory(
        listing_id=ad_id,
        old_trim=old_trim,
        new_trim=normalized_trim,
        changed_by=changed_by,
        changed_at=datetime.utcnow()
    )
    db.add(history)

    # Optional alias creation
    if payload.get("create_alias") and payload.get("alias_text") and trim_master_id:
        alias_text = payload["alias_text"].strip().lower()
        existing = db.query(models.TrimAlias).filter(
            models.TrimAlias.trim_master_id == trim_master_id,
            models.TrimAlias.alias == alias_text
        ).first()
        if not existing:
            new_alias = models.TrimAlias(trim_master_id=trim_master_id, alias=alias_text)
            db.add(new_alias)

    db.commit()
    return {"status": "ok", "ad_id": ad_id, "normalized_trim": normalized_trim}


# ----------------------
# Reprocess a listing through matching logic
# ----------------------
@router.post("/listings/{ad_id}/reprocess")
def reprocess_listing(ad_id: str, db: Session = Depends(get_db)):
    listing = db.query(models.Listings).filter(models.Listings.ad_id == ad_id).first()
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    candidate_trims = matching.get_candidate_trims(db, listing.brand, listing.model)
    listing_obj = type("Obj", (object,), {
        "raw_trim": listing.trim,
        "brand": listing.brand,
        "model": listing.model
    })
    match_result = matching.match_trim(listing_obj, candidate_trims)

    old_trim = listing.normalized_trim
    listing.normalized_trim = match_result.get("trim")
    listing.trim_confidence = match_result.get("confidence", 0.0)
    listing.assignment_method = match_result.get("assignment_method", "unmatched")
    listing.needs_review = (listing.assignment_method == "unmatched")
    listing.processed_at = datetime.utcnow()

    # record history
    history = models.TrimHistory(
        listing_id=ad_id,
        old_trim=old_trim,
        new_trim=match_result.get("trim"),
        changed_by="system_reprocess",
        changed_at=datetime.utcnow()
    )
    db.add(history)

    db.commit()
    return {"status": "ok", "ad_id": ad_id, "match_result": match_result}


# ----------------------
# Get candidate trims for a listing (with scores)
# ----------------------
@router.get("/listings/{ad_id}/candidates")
def get_listing_candidates(ad_id: str, top_n: int = 10, db: Session = Depends(get_db)):
    listing = db.query(models.Listings).filter(models.Listings.ad_id == ad_id).first()
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    candidate_trims = matching.get_candidate_trims(db, listing.brand, listing.model)
    from rapidfuzz import process, fuzz
    if not listing.trim or not candidate_trims:
        return {"candidates": []}

    results = process.extract(
        listing.trim,
        candidate_trims,
        scorer=fuzz.token_sort_ratio,
        limit=top_n
    )
    return [{"trim": r[0], "score": r[1] / 100.0} for r in results]


# ----------------------
# Mark listing as reviewed (no changes)
# ----------------------
@router.post("/listings/{ad_id}/reviewed")
def mark_reviewed(ad_id: str, changed_by: Optional[str] = Body(None), db: Session = Depends(get_db)):
    listing = db.query(models.Listings).filter(models.Listings.ad_id == ad_id).first()
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
    listing.needs_review = False
    listing.last_reviewed_at = datetime.utcnow()
    db.commit()
    return {"status": "ok", "ad_id": ad_id}


# ----------------------
# Stats endpoint
# ----------------------
@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    total = db.query(models.Listings).count()
    processed = db.query(models.Listings).filter(models.Listings.processed_at.isnot(None)).count()
    needs_review = db.query(models.Listings).filter(models.Listings.needs_review.is_(True)).count()
    return {"total": total, "processed": processed, "needs_review": needs_review}
