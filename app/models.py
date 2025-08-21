from sqlalchemy import Column, Integer, String, Text, Boolean, Float, TIMESTAMP, ForeignKey
from .database import Base

class TrimMaster(Base):
    __tablename__ = "trim_master"

    id = Column(Integer, primary_key=True, index=True)
    make = Column(String, nullable=False)
    model = Column(String, nullable=False)
    trim_name = Column(String, nullable=False)
    year_start = Column(Integer, nullable=True)
    year_end = Column(Integer, nullable=True)
    created_at = Column(TIMESTAMP)

class TrimAlias(Base):
    __tablename__ = "trim_alias"

    id = Column(Integer, primary_key=True, index=True)
    trim_master_id = Column(Integer, ForeignKey("trim_master.id"), nullable=False)
    alias = Column(String, nullable=False)
    created_at = Column(TIMESTAMP)

class Listings(Base):
    __tablename__ = "listings"

    ad_id = Column(String, primary_key=True, index=True)
    title = Column(String)
    brand = Column(String)
    model = Column(String)
    year = Column(Integer)
    website = Column(String)
    trim = Column(String)
    website = Column(String)
    normalized_trim = Column(String)
    trim_confidence = Column(Float)
    assignment_method = Column(String)
    needs_review = Column(Boolean, default=False)
    last_reviewed_at = Column(TIMESTAMP)
    processed_at = Column(TIMESTAMP)

class TrimHistory(Base):
    __tablename__ = "trim_history"

    id = Column(Integer, primary_key=True, index=True)
    listing_id = Column(String, ForeignKey("listings.ad_id"), nullable=False)
    old_trim = Column(String)
    new_trim = Column(String, nullable=False)
    changed_by = Column(String, nullable=False)
    changed_at = Column(TIMESTAMP)
