from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from app.database import SessionLocal
from app import models, schemas
router = APIRouter()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----- GET /trims -----

@router.get("/trims", response_model=List[schemas.Trim])
def list_trims(
    make: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(get_db)
):

    query = db.query(models.TrimMaster)

    # normalize function: lowercase + strip non-alphanumeric
    normalize = lambda col: func.regexp_replace(func.lower(col), '[^a-z0-9]', '', 'g')

    if make:
        query = query.filter(
            normalize(models.TrimMaster.make) == func.regexp_replace(func.lower(make), '[^a-z0-9]', '', 'g')
        )
    if model:
        query = query.filter(
            normalize(models.TrimMaster.model) == func.regexp_replace(func.lower(model), '[^a-z0-9]', '', 'g')
        )

    trims = query.offset(skip).limit(limit).all()
    return trims
# ----- POST /trims -----
@router.post("/trims", response_model=schemas.Trim)
def add_trim(trim_in: schemas.TrimCreate, db: Session = Depends(get_db)):
    # Prevent duplicates
    existing = db.query(models.TrimMaster).filter_by(
        make=trim_in.make,
        model=trim_in.model,
        trim_name=trim_in.trim_name
    ).first()

    if existing:
        raise HTTPException(status_code=400, detail="Trim already exists")

    trim = models.TrimMaster(
        make=trim_in.make,
        model=trim_in.model,
        trim_name=trim_in.trim_name,
        year_start=trim_in.year_start,
        year_end=trim_in.year_end
    )
    db.add(trim)
    db.commit()
    db.refresh(trim)
    return trim

