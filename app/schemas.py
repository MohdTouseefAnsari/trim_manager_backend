from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

# ----------------------
# Alias schemas
# ----------------------
class AliasBase(BaseModel):
    trim_master_id: int
    alias: str

class AliasCreate(AliasBase):
    pass

class Alias(AliasBase):
    id: int
    make: str
    model: str
    trim_name: str
    created_at: Optional[datetime]

    class Config:
        from_attributes = True  # Pydantic v2 replacement for orm_mode

# ----------------------
# Trim schemas
# ----------------------
class TrimBase(BaseModel):
    make: str
    model: str
    trim_name: str
    year_start: Optional[int] = None
    year_end: Optional[int] = None

class TrimCreate(TrimBase):
    pass

class Trim(TrimBase):
    id: int
    created_at: Optional[datetime]

    class Config:
        from_attributes = True

# ----------------------
# Listing schemas
# ----------------------
class ListingBase(BaseModel):
    ad_id: str
    brand: str
    model: str
    year: Optional[int]
    raw_trim: Optional[str] = None  # maps to Listing.trim column

class UnprocessedListing(ListingBase):
    pass

class ProcessedListing(ListingBase):
    normalized_trim: Optional[str] = None
    trim_confidence: Optional[float] = None
    assignment_method: Optional[str] = None
    needs_review: bool
    processed_at: Optional[datetime] = None

# ----------------------
# Assign trim request
# ----------------------
class AssignTrimRequest(BaseModel):
    trim_master_id: Optional[int] = None
    normalized_trim: Optional[str] = None
    confidence: Optional[float] = 1.0
    changed_by: Optional[str] = "manual"
    create_alias: Optional[bool] = False
    alias_text: Optional[str] = None

# ----------------------
# Candidate trims
# ----------------------
class CandidateTrim(BaseModel):
    trim: str
    score: float

class CandidatesResponse(BaseModel):
    candidates: List[CandidateTrim]

# ----------------------
# Stats response
# ----------------------
class StatsResponse(BaseModel):
    total: int
    processed: int
    needs_review: int
