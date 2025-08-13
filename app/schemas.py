from pydantic import BaseModel
from typing import Optional
from datetime import datetime

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
        from_attributes = True


from pydantic import BaseModel
from typing import Optional
from datetime import datetime

# ----- Trim Schemas -----

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
        from_attributes = True  # Pydantic v2 replacement for orm_mode
