from database import SessionLocal
from models import DistrictData

db = SessionLocal()
records = db.query(DistrictData).limit(5).all()

for r in records:
    print(r.district_name, r.state_name, r.total_persondays)

db.close()
