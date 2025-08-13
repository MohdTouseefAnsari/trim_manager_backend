from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import List, Optional
from app.database import SessionLocal
from app import models, schemas

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/aliases", response_model=List[schemas.Alias])
def list_aliases(
    make: Optional[str] = Query(None),
    model: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(
        models.TrimAlias.id,
        models.TrimAlias.trim_master_id,
        models.TrimAlias.alias,
        models.TrimAlias.created_at,
        models.TrimMaster.make,
        models.TrimMaster.model,
        models.TrimMaster.trim_name,
    ).join(models.TrimMaster)

    if make:
        query = query.filter(models.TrimMaster.make.ilike(make))
    if model:
        query = query.filter(models.TrimMaster.model.ilike(model))

    aliases = query.all()

    # Convert tuples to dict for Pydantic model
    result = [
        schemas.Alias(
            id=row.id,
            trim_master_id=row.trim_master_id,
            alias=row.alias,
            created_at=row.created_at,
            make=row.make,
            model=row.model,
            trim_name=row.trim_name
        )
        for row in aliases
    ]
    return result
@router.post("/aliases", response_model=schemas.Alias)
def add_alias(alias_in: schemas.AliasCreate, db: Session = Depends(get_db)):   # ✅ FIXED
    trim = db.query(models.TrimMaster).filter(models.TrimMaster.id == alias_in.trim_master_id).first()
    if not trim:
        raise HTTPException(status_code=404, detail="TrimMaster not found")

    existing = db.query(models.TrimAlias).filter(
        models.TrimAlias.trim_master_id == alias_in.trim_master_id,
        models.TrimAlias.alias == alias_in.alias.lower()
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Alias already exists for this trim")

    alias_obj = models.TrimAlias(
        trim_master_id=alias_in.trim_master_id,
        alias=alias_in.alias.lower()
    )
    db.add(alias_obj)
    db.commit()
    db.refresh(alias_obj)

    return schemas.Alias(
        id=alias_obj.id,
        trim_master_id=alias_obj.trim_master_id,
        alias=alias_obj.alias,
        created_at=alias_obj.created_at,
        make=trim.make,
        model=trim.model,
        trim_name=trim.trim_name
    )

@router.delete("/aliases/{alias_id}")
def delete_alias(alias_id: int, db: Session = Depends(get_db)):   # ✅ FIXED
    alias = db.query(models.TrimAlias).filter(models.TrimAlias.id == alias_id).first()
    if not alias:
        raise HTTPException(status_code=404, detail="Alias not found")

    db.delete(alias)
    db.commit()
    return {"message": "Alias deleted successfully"}
