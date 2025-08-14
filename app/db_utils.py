from sqlalchemy.orm import Session
from datetime import datetime
from app import models

# ----------------------
# Get unprocessed listings
# ----------------------
def get_unprocessed_listings(db: Session, limit: int = 500, offset: int = 0):
    """
    Fetch listings that are either unprocessed or marked as needing review.
    Returns a list of tuples: (ad_id, brand, model, year, trim)
    """
    rows = (
        db.query(
            models.Listings.ad_id,
            models.Listings.brand,
            models.Listings.model,
            models.Listings.year,
            models.Listings.trim
        )
        .filter(
            (models.Listings.processed_at.is_(None)) |
            (models.Listings.needs_review.is_(True))
        )
        .offset(offset)
        .limit(limit)
        .all()
    )
    return rows


# ----------------------
# Update listing after matching
# ----------------------
def update_listing_with_match(
    db: Session,
    ad_id: str,
    normalized_trim: str = None,
    confidence: float = 0.0,
    method: str = "unmatched"
):
    """
    Updates a listing with the matched trim information and logs history.
    Ensures that normalized_trim and new_trim are never None.
    """
    listing = db.query(models.Listings).filter(models.Listings.ad_id == ad_id).first()
    if not listing:
        return None

    # Preserve old trim
    old_trim = listing.normalized_trim or listing.trim or "unmatched"

    # Set new trim safely
    safe_trim = normalized_trim or listing.trim or "unmatched"

    listing.normalized_trim = safe_trim
    listing.trim_confidence = confidence
    listing.assignment_method = method
    listing.needs_review = (method == "unmatched")
    listing.processed_at = datetime.utcnow()

    # Record history
    history = models.TrimHistory(
        listing_id=ad_id,
        old_trim=old_trim,
        new_trim=safe_trim,
        changed_by="system_match",
        changed_at=datetime.utcnow()
    )
    db.add(history)
    db.commit()
    return listing


# ----------------------
# Optional helper: bulk update
# ----------------------
def bulk_update_listings(db: Session, updates: list[dict]):
    """
    Accepts a list of updates like:
    [{"ad_id": "123", "normalized_trim": "SE", "confidence": 0.9, "method": "exact"}, ...]
    """
    for update in updates:
        update_listing_with_match(
            db,
            ad_id=update["ad_id"],
            normalized_trim=update.get("normalized_trim"),
            confidence=update.get("confidence", 0.0),
            method=update.get("method", "unmatched")
        )
