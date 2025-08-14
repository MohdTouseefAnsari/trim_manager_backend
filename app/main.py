from fastapi import FastAPI
from app import models
from app.database import engine
from app.routes import aliases,trims,listings
from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:5173",
    "http://frontend:5173"
]



models.Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(aliases.router, tags=["Aliases"])
app.include_router(trims.router, tags=["Trims"])
app.include_router(listings.router, tags = ["Listings"])
