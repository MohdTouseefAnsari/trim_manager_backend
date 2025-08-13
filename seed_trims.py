import csv
import time
from datetime import datetime
from sqlalchemy.orm import Session
from app.database import SessionLocal, engine, Base
from app import models

CSV_PATH = "car_trims.csv"

Base.metadata.create_all(bind=engine)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def seed_trim_master():
    start_time = time.time()
    db: Session = SessionLocal()

    skipped_rows = []
    count_added = 0
    seen = set()  # Track make/model/trim already processed in this run

    log(f"Loading trims from {CSV_PATH}")
    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for idx, row in enumerate(reader, start=1):
            make = row['Make'].strip()
            model = row['Model'].strip()
            trim_name = row['Trim'].strip()

            if not trim_name:
                skipped_rows.append((make, model))
                continue

            key = (make.lower(), model.lower(), trim_name.lower())
            if key in seen:
                continue  # Avoid duplicate insert in same run
            seen.add(key)

            # Check DB for existing trim
            existing = db.query(models.TrimMaster).filter_by(
                make=make,
                model=model,
                trim_name=trim_name
            ).first()

            if not existing:
                trim_obj = models.TrimMaster(
                    make=make,
                    model=model,
                    trim_name=trim_name
                )
                db.add(trim_obj)
                db.flush()  # Assigns ID without commit

                alias = models.TrimAlias(
                    trim_master_id=trim_obj.id,
                    alias=trim_name.lower()
                )
                db.add(alias)

                count_added += 1

            if idx % 500 == 0:
                log(f"Processed {idx} rows, added {count_added} trims so far")

    db.commit()
    db.close()

    elapsed = time.time() - start_time
    log(f"✅ Finished: {count_added} trims added in {elapsed:.2f} seconds.")
    if skipped_rows:
        log(f"⚠️ Skipped {len(skipped_rows)} rows with empty trims.")
        for make, model in skipped_rows[:10]:  # Only show first 10 skipped
            log(f"   - {make} {model}")
        if len(skipped_rows) > 10:
            log(f"   ... +{len(skipped_rows)-10} more skipped")


if __name__ == "__main__":
    seed_trim_master()
