# app/routes/listings.py
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional, List
from datetime import datetime
from sqlalchemy import text, func, case
from app.database import SessionLocal
from app import models, db_utils, matching
from collections import Counter


import asyncio
import httpx


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


# app/routes/listings.py
WEBSITE_TABLE_MAP = {
    "dubizzle": "dubizzle_details",
    "carswitch": "carswitch_details",
    "syarah": "syarah_details",
    "opensooq": "opensooq_details",
}

@router.get("/listings/{ad_id}/details")
def get_full_listing(ad_id: str, db: Session = Depends(get_db)):
    # 1. Fetch the listing
    listing = db.query(models.Listings).filter(models.Listings.ad_id == ad_id).first()
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    website = listing.website.lower()
    if website not in WEBSITE_TABLE_MAP:
        raise HTTPException(status_code=400, detail=f"No details table for website {website}")

    details_table = WEBSITE_TABLE_MAP[website]

    # 2. Fetch website-specific details
    details = db.execute(
    text(f"SELECT * FROM {details_table} WHERE ad_id = :lid"),
    {"lid": ad_id}).mappings().first()

    # 3. Combine base listing + details for frontend
    listing_data = {
        "ad_id": listing.ad_id,
        "brand": listing.brand,
        "model": listing.model,
        "year": listing.year,
        "trim": listing.trim,
        "normalized_trim": listing.normalized_trim,
        "confidence": listing.trim_confidence,
        "method": listing.assignment_method,
        "needs_review": listing.needs_review,
        "processed_at": listing.processed_at,
        "last_reviewed_at": listing.last_reviewed_at,
    }

    return {
        "listing": listing_data,
        "details": dict(details) if details else {}
    }


@router.post("/process-listings")
def process_listings(
    db: Session = Depends(get_db),
    limit: int = Query(None, ge=1),
    batch_size: int = Query(100, ge=10)
):
    """
    Process unprocessed listings in batches.
    Limit: maximum number of unprocessed listings to process
    Batch_size: how many to commit at a time
    """
    unprocessed = db_utils.get_unprocessed_listings(db, limit)

    if not unprocessed:
        return {"status": "ok", "message": "No unprocessed listings found"}

    stats_methods = Counter()
    stats_confidence = Counter()
    processed_count = 0

    for listing in unprocessed:
        ad_id, brand, model, year, trim, title, website = listing
        candidate_trims = matching.get_candidate_trims(db, brand, model)

        website = website.lower()
        details_table = WEBSITE_TABLE_MAP[website]

        #
        details = db.execute(
            text(f"SELECT * FROM {details_table} WHERE ad_id = :lid"),
            {"lid": ad_id}
        ).mappings().first()

        description = details["description"] if details and "description" in details else None

        # temporary object for matching
        listing_obj = type("Obj", (object,), {
            "title": title,
            "trim": trim,
            "brand": brand,
            "model": model,
            "description": description
        })

        match_result = matching.match_trim(
            listing=listing_obj,
            candidate_trims=candidate_trims,
            allow_external_llm=True,
            fuzzy_primary_threshold=82,
            fuzzy_secondary_threshold=74,
            min_ai_confidence=0.55,
        )

        method = match_result.get("assignment_method", "unmatched")
        stats_methods[method] += 1

        conf = match_result.get("confidence", 0.0)
        if conf >= 0.75:
            stats_confidence["high"] += 1
        elif conf >= 0.4:
            stats_confidence["medium"] += 1
        else:
            stats_confidence["low"] += 1

        # update listing & record history
        db_utils.update_listing_with_match(
            db=db,
            ad_id=ad_id,
            normalized_trim=match_result.get("trim"),
            confidence=conf,
            method=method
        )

        processed_count += 1

        # Commit every batch_size
        if processed_count % batch_size == 0:
            db.commit()

    # Final commit
    db.commit()

    return {
        "status": "ok",
        "processed": processed_count,
        "methods": dict(stats_methods),
        "confidence": dict(stats_confidence)
    }
# # ----------------------
# # Process unprocessed listings
# # ----------------------
# @router.post("/process-listings")
# def process_listings(limit: int = 5, db: Session = Depends(get_db)):
#     unprocessed = db_utils.get_unprocessed_listings(db, limit)

#     processed_count = 0
#     exact_matches = 0
#     fuzzy_matches = 0
#     llm_matches = 0
#     unmatched = 0

#     for listing in unprocessed:
#         ad_id, brand, model, year, trim, title, website = listing
#         candidate_trims = matching.get_candidate_trims(db, brand, model)

#         website = website.lower()
#         details_table = WEBSITE_TABLE_MAP[website]

#     # 2. Fetch website-specific details
#         details = db.execute(
#         text(f"SELECT * FROM {details_table} WHERE ad_id = :lid"),
#         {"lid": ad_id}).mappings().first()

#         description = None
#         if details and "description" in details:
#             description = details['description']
#             print("Description:", details["description"])
#         else:
#             print("No description column or no result")


#         # temporary object for matching
#         listing_obj = type("Obj", (object,), {
#             "title": title,
#             "trim": trim,  # provide .trim to matching.match_trim
#             "brand": brand,
#             "model": model,
#             "description": description
#         })



#         match_result = matching.match_trim(
#     listing=listing_obj,
#     candidate_trims=candidate_trims,
#     allow_external_llm=True,          # or False to run cheaper/faster passes
#     fuzzy_primary_threshold=82,
#     fuzzy_secondary_threshold=74,
#     min_ai_confidence=0.55,
# )

#         method = match_result.get("assignment_method", "unmatched")
#         if method == "exact":
#             exact_matches += 1
#         elif method == "fuzzy":
#             fuzzy_matches += 1
#         elif method == "LLM":
#             llm_matches += 1
#         else:
#             unmatched += 1

#         # update listing & record history
#         db_utils.update_listing_with_match(
#             db=db,
#             ad_id=ad_id,
#             normalized_trim=match_result.get("trim"),
#             confidence=match_result.get("confidence", 0.0),
#             method=method
#         )

#         processed_count += 1

#     return {
#         "processed": processed_count,
#         "exact_matches": exact_matches,
#         "fuzzy_matches": fuzzy_matches,
#         "llm_matches": llm_matches,
#         "unmatched": unmatched
#     }


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

    rows = q.all()
    return [
        {
            "ad_id": r.ad_id,
            "brand": r.brand,
            "model": r.model,
            "year": r.year,
            "trim": r.trim,  # consistent key for frontend
            "normalized_trim": r.normalized_trim,
            "confidence": r.trim_confidence,
            "method": r.assignment_method,
            "needs_review": r.needs_review,
            "processed_at": r.processed_at
        }
        for r in rows
    ]






# ----------------------
# Manually assign trim
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
        "trim": listing.trim,  # changed from raw_trim
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





# ----------------------
# Detailed stats endpoint
# ----------------------

@router.get("/stats/detailed")
def get_detailed_stats(db: Session = Depends(get_db)):
    total = db.query(models.Listings).count()
    processed = db.query(models.Listings).filter(models.Listings.processed_at.isnot(None)).count()
    needs_review = db.query(models.Listings).filter(models.Listings.needs_review.is_(True)).count()

    # Count by assignment_method
    method_counts = (
        db.query(
            models.Listings.assignment_method,
            func.count().label("count")
        )
        .group_by(models.Listings.assignment_method)
        .all()
    )

    # Confidence distribution (low/med/high)
    conf_buckets = (
        db.query(
            func.count(case((models.Listings.trim_confidence < 0.5, 1))).label("low_conf"),
            func.count(case(((models.Listings.trim_confidence >= 0.5) & (models.Listings.trim_confidence < 0.8), 1))).label("medium_conf"),
            func.count(case((models.Listings.trim_confidence >= 0.8, 1))).label("high_conf"),
        )
        .first()
    )

    # Average confidence overall
    avg_conf = db.query(func.avg(models.Listings.trim_confidence)).scalar()

    # Breakdown by brand (top 10)
    brand_breakdown = (
        db.query(
            models.Listings.brand,
            func.count().label("count"),
            func.avg(models.Listings.trim_confidence).label("avg_conf"),
        )
        .group_by(models.Listings.brand)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )

    return {
        "summary": {
            "total": total,
            "processed": processed,
            "needs_review": needs_review,
            "avg_confidence": round(avg_conf or 0, 3),
        },
        "methods": {m: c for m, c in method_counts},
        "confidence": {
            "low": conf_buckets.low_conf,
            "medium": conf_buckets.medium_conf,
            "high": conf_buckets.high_conf,
        },
        "brands_top10": [
            {"brand": b, "count": c, "avg_conf": round(ac or 0, 3)}
            for b, c, ac in brand_breakdown
        ]
    }


# async def process_listing(listing, db: Session):
#     """Process a single listing using existing matching logic."""
#     candidate_trims = matching.get_candidate_trims(db, listing.brand, listing.model)
#     listing_obj = type("Obj", (object,), {
#         "trim": listing.trim,
#         "brand": listing.brand,
#         "model": listing.model,
#         "title": getattr(listing, "title", ""),
#         "description": getattr(listing, "description", "")
#     })
#     # Use existing match_trim (sync), can still call async LLM internally if needed
#     match_result = matching.match_trim(listing_obj, candidate_trims)

#     old_trim = listing.normalized_trim

#     listing.normalized_trim = match_result.get("trim")
#     listing.trim_confidence = match_result.get("confidence", 0.0)
#     listing.assignment_method = match_result.get("assignment_method", "unmatched")
#     listing.needs_review = (listing.assignment_method == "unmatched")
#     listing.processed_at = datetime.utcnow()

#     history = models.TrimHistory(
#         listing_id=listing.ad_id,
#         old_trim=old_trim,
#         new_trim=match_result.get("trim"),
#         changed_by="system_bulk_reprocess_async",
#         changed_at=datetime.utcnow()
#     )
#     db.add(history)

#     return match_result

# async def process_batch(batch, db: Session):
#     """Process a batch of listings concurrently."""
#     tasks = [asyncio.to_thread(process_listing, listing, db) for listing in batch]
#     results = await asyncio.gather(*tasks)
#     db.commit()  # commit once per batch
#     return results

# @router.post("/reprocess-processed-async")
# async def reprocess_processed_async(
#     db: Session = Depends(get_db),
#     limit: int = Query(None, ge=1),
#     batch_size: int = Query(50, ge=10)  # smaller batch_size for async safety
# ):
#     query = db.query(models.Listings).filter(models.Listings.processed_at.isnot(None))
#     if limit:
#         query = query.limit(limit)
#     listings = query.all()
#     if not listings:
#         return {"status": "ok", "message": "No processed listings found"}

#     stats_methods = Counter()
#     stats_confidence = Counter()
#     processed_count = 0

#     # Split listings into batches
#     for i in range(0, len(listings), batch_size):
#         batch = listings[i:i+batch_size]
#         results = await process_batch(batch, db)
#         # Update stats
#         for res, listing in zip(results, batch):
#             stats_methods[listing.assignment_method] += 1
#             conf = listing.trim_confidence
#             if conf >= 0.75:
#                 stats_confidence["high"] += 1
#             elif conf >= 0.4:
#                 stats_confidence["medium"] += 1
#             else:
#                 stats_confidence["low"] += 1
#             processed_count += 1

#     return {
#         "status": "ok",
#         "processed": processed_count,
#         "methods": dict(stats_methods),
#         "confidence": dict(stats_confidence)
#     }

@router.post("/reprocess-processed")
def reprocess_processed(
    db: Session = Depends(get_db),
    brand: str = Query(None),
    model: str = Query(None),
    limit: int = Query(None, ge=1),
    batch_size: int = Query(100, ge=10)
):
    """
    Reprocess already processed listings with updated matching logic.
    Optional filters: brand, model
    Limit: maximum number of listings to reprocess
    Batch_size: how many to commit at a time
    """
    query = db.query(models.Listings).filter(models.Listings.processed_at.isnot(None))
    if brand:
        query = query.filter(models.Listings.brand.ilike(brand))
    if model:
        query = query.filter(models.Listings.model.ilike(model))
    if limit:
        query = query.limit(limit)

    listings = query.all()
    if not listings:
        return {"status": "ok", "message": "No processed listings found for the given filters"}

    stats_methods = Counter()
    stats_confidence = Counter()
    processed_count = 0

    for listing in listings:
        candidate_trims = matching.get_candidate_trims(db, listing.brand, listing.model)
        listing_obj = type("Obj", (object,), {
            "trim": listing.trim,
            "brand": listing.brand,
            "model": listing.model,
            "title": getattr(listing, "title", ""),
            "description": getattr(listing, "description", "")
        })
        match_result = match_result = matching.match_trim(
    listing=listing_obj,
    candidate_trims=candidate_trims,
    allow_external_llm=True,          # or False to run cheaper/faster passes
    fuzzy_primary_threshold=82,
    fuzzy_secondary_threshold=74,
    min_ai_confidence=0.55,
)

        old_trim = listing.normalized_trim

        listing.normalized_trim = match_result.get("trim")
        listing.trim_confidence = match_result.get("confidence", 0.0)
        listing.assignment_method = match_result.get("assignment_method", "unmatched")
        listing.needs_review = (listing.assignment_method == "unmatched")
        listing.processed_at = datetime.utcnow()

        new_trim = match_result.get("trim") or ""

        history = models.TrimHistory(
            listing_id=listing.ad_id,
            old_trim=old_trim,
            new_trim=new_trim,
            changed_by="system_bulk_reprocess",
            changed_at=datetime.utcnow()
        )
        db.add(history)

        stats_methods[listing.assignment_method] += 1
        conf = listing.trim_confidence
        if conf >= 0.75:
            stats_confidence["high"] += 1
        elif conf >= 0.4:
            stats_confidence["medium"] += 1
        else:
            stats_confidence["low"] += 1

        processed_count += 1

        # Commit every batch_size records
        if processed_count % batch_size == 0:
            db.commit()

    db.commit()
    return {
        "status": "ok",
        "processed": processed_count,
        "methods": dict(stats_methods),
        "confidence": dict(stats_confidence)
    }
# # ----------------------
# # Bulk reprocess processed listings
# # ----------------------
# @router.post("/reprocess-processed")
# def reprocess_processed_listings(
#     limit: int = Query(500, ge=1, le=5000),  # batch size
#     db: Session = Depends(get_db)
# ):
#     """
#     Reprocess listings that were already processed before.
#     Skips manual assignments.
#     """
#     # 1. Fetch already processed (but not manual) listings
#     listings = (
#         db.query(models.Listings)
#         .filter(
#             models.Listings.processed_at.isnot(None),
#             models.Listings.assignment_method != "manual"
#         )
#         .limit(limit)
#         .all()
#     )

#     if not listings:
#         return {"status": "done", "message": "No eligible listings found"}

#     reprocessed_count = 0
#     method_counts = {"exact": 0, "fuzzy": 0, "LLM": 0, "unmatched": 0}

#     for listing in listings:
#         candidate_trims = matching.get_candidate_trims(db, listing.brand, listing.model)

#         # Build lightweight object for matcher
#         listing_obj = type("Obj", (object,), {
#             "trim": listing.trim,
#             "brand": listing.brand,
#             "model": listing.model,
#             "title": getattr(listing, "title", None),
#             "description": getattr(listing, "description", None)
#         })

#         match_result = matching.match_trim(listing_obj, candidate_trims)

#         # Record old trim
#         old_trim = listing.normalized_trim

#         # Update listing
#         listing.normalized_trim = match_result.get("trim")
#         listing.trim_confidence = match_result.get("confidence", 0.0)
#         listing.assignment_method = match_result.get("assignment_method", "unmatched")
#         listing.needs_review = (listing.assignment_method == "unmatched")
#         listing.processed_at = datetime.utcnow()

#         # Count methods
#         method = listing.assignment_method
#         if method in method_counts:
#             method_counts[method] += 1
#         else:
#             method_counts["unmatched"] += 1

#         # Record history
#         history = models.TrimHistory(
#             listing_id=listing.ad_id,
#             old_trim=old_trim,
#             new_trim=listing.normalized_trim,
#             changed_by="system_bulk_reprocess",
#             changed_at=datetime.utcnow()
#         )
#         db.add(history)

#         reprocessed_count += 1

#     db.commit()

#     return {
#         "status": "ok",
#         "reprocessed": reprocessed_count,
#         "method_counts": method_counts
#     }
