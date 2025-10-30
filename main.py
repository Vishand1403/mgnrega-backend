from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from fastapi.middleware.cors import CORSMiddleware
import requests
from datetime import datetime, timedelta  # ✅ Added for cache timing

app = FastAPI()

origins = [
    "https://mgnrega-frontend-nw6c.vercel.app",  # ✅ your live frontend
    "http://localhost:5173",                     # ✅ local dev
    "http://127.0.0.1:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Database setup
DATABASE_URL = "sqlite:///./mgnrega.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

# ✅ Database model
class DistrictData(Base):
    __tablename__ = "district_data"
    id = Column(Integer, primary_key=True, index=True)
    state_name = Column(String)
    district_name = Column(String)
    fin_year = Column(String)
    month = Column(String)
    Approved_Labour_Budget = Column(Float)
    Average_Wage_rate_per_day_per_person = Column(Float)
    Average_days_of_employment_provided_per_Household = Column(Float)
    Differently_abled_persons_worked = Column(Float)
    Material_and_skilled_Wages = Column(Float)
    Number_of_Completed_Works = Column(Float)
    Number_of_GPs_with_NIL_exp = Column(Float)
    Number_of_Ongoing_Works = Column(Float)
    Persondays_of_Central_Liability_so_far = Column(Float)
    SC_persondays = Column(Float)
    ST_persondays = Column(Float)
    Total_Adm_Expenditure = Column(Float)
    Total_Exp = Column(Float)
    Total_Households_Worked = Column(Float)
    Total_Individuals_Worked = Column(Float)
    Total_No_of_Active_Job_Cards = Column(Float)
    Total_No_of_Active_Workers = Column(Float)
    Total_No_of_HHs_completed_100_Days_of_Wage_Employment = Column(Float)
    Total_No_of_JobCards_issued = Column(Float)
    Total_No_of_Workers = Column(Float)
    Total_No_of_Works_Takenup = Column(Float)
    Wages = Column(Float)
    Women_Persondays = Column(Float)
    percent_of_Category_B_Works = Column(Float)
    percent_of_Expenditure_on_Agriculture_Allied_Works = Column(Float)
    percent_of_NRM_Expenditure = Column(Float)
    percentage_payments_generated_within_15_days = Column(Float)

Base.metadata.create_all(bind=engine)

# ✅ Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def home():
    return {"message": "MGNREGA Tamil Nadu backend active ✅"}


# 🧠 CACHE SETUP
cache_data = {
    "timestamp": None,
    "data": None
}
CACHE_DURATION = timedelta(hours=6)  # cache valid for 6 hours

def is_cache_valid():
    """Return True if cache exists and is not expired"""
    if cache_data["timestamp"] is None:
        return False
    return datetime.now() - cache_data["timestamp"] < CACHE_DURATION


# ✅ Fetch and store data (with caching)
@app.post("/fetch_tn_data")
def fetch_tn_data(db: Session = Depends(get_db)):
    # 🔹 Return cached result if still valid
    if is_cache_valid():
        print("✅ Using cached MGNREGA data.")
        return {"message": "Using cached data (from memory)", "cached": True}

    print("⏳ Fetching fresh data from API...")

    base_url = "https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
    api_key = "579b464db66ec23bdd0000012ebcd9f379884598719738fb876815ef"

    years = [
        "2018-2019", "2019-2020", "2020-2021",
        "2021-2022", "2022-2023", "2023-2024", "2024-2025"
    ]

    total = 0
    all_records = []

    for year in years:
        params = {
            "api-key": api_key,
            "format": "json",
            "limit": 1000,
            "filters[state_name]": "TAMIL NADU",
            "filters[fin_year]": year
        }
        res = requests.get(base_url, params=params)
        if res.status_code != 200:
            print(f"❌ {year} failed: {res.status_code}")
            continue

        for r in res.json().get("records", []):
            entry = DistrictData(
                state_name=r.get("state_name"),
                district_name=r.get("district_name"),
                fin_year=year,
                month=r.get("month"),
                Approved_Labour_Budget=float(r.get("Approved_Labour_Budget", 0) or 0),
                Average_Wage_rate_per_day_per_person=float(r.get("Average_Wage_rate_per_day_per_person", 0) or 0),
                Average_days_of_employment_provided_per_Household=float(r.get("Average_days_of_employment_provided_per_Household", 0) or 0),
                Differently_abled_persons_worked=float(r.get("Differently_abled_persons_worked", 0) or 0),
                Material_and_skilled_Wages=float(r.get("Material_and_skilled_Wages", 0) or 0),
                Number_of_Completed_Works=float(r.get("Number_of_Completed_Works", 0) or 0),
                Number_of_GPs_with_NIL_exp=float(r.get("Number_of_GPs_with_NIL_exp", 0) or 0),
                Number_of_Ongoing_Works=float(r.get("Number_of_Ongoing_Works", 0) or 0),
                Persondays_of_Central_Liability_so_far=float(r.get("Persondays_of_Central_Liability_so_far", 0) or 0),
                SC_persondays=float(r.get("SC_persondays", 0) or 0),
                ST_persondays=float(r.get("ST_persondays", 0) or 0),
                Total_Adm_Expenditure=float(r.get("Total_Adm_Expenditure", 0) or 0),
                Total_Exp=float(r.get("Total_Exp", 0) or 0),
                Total_Households_Worked=float(r.get("Total_Households_Worked", 0) or 0),
                Total_Individuals_Worked=float(r.get("Total_Individuals_Worked", 0) or 0),
                Total_No_of_Active_Job_Cards=float(r.get("Total_No_of_Active_Job_Cards", 0) or 0),
                Total_No_of_Active_Workers=float(r.get("Total_No_of_Active_Workers", 0) or 0),
                Total_No_of_HHs_completed_100_Days_of_Wage_Employment=float(r.get("Total_No_of_HHs_completed_100_Days_of_Wage_Employment", 0) or 0),
                Total_No_of_JobCards_issued=float(r.get("Total_No_of_JobCards_issued", 0) or 0),
                Total_No_of_Workers=float(r.get("Total_No_of_Workers", 0) or 0),
                Total_No_of_Works_Takenup=float(r.get("Total_No_of_Works_Takenup", 0) or 0),
                Wages=float(r.get("Wages", 0) or 0),
                Women_Persondays=float(r.get("Women_Persondays", 0) or 0),
                percent_of_Category_B_Works=float(r.get("percent_of_Category_B_Works", 0) or 0),
                percent_of_Expenditure_on_Agriculture_Allied_Works=float(r.get("percent_of_Expenditure_on_Agriculture_Allied_Works", 0) or 0),
                percent_of_NRM_Expenditure=float(r.get("percent_of_NRM_Expenditure", 0) or 0),
                percentage_payments_generated_within_15_days=float(r.get("percentage_payments_generated_within_15_days", 0) or 0),
            )
            db.add(entry)
            all_records.append(entry)
            total += 1

        db.commit()
        print(f"✅ {year} → {total} total inserted so far")

    # 🧠 Save to cache
    cache_data["timestamp"] = datetime.now()
    cache_data["data"] = all_records

    return {"message": f"✅ {total} Tamil Nadu records stored successfully", "cached": False}


# ✅ Get filtered data (Frontend Compatible)
@app.get("/get_data")
def get_data(state_name: str, fin_year: str = "All", db: Session = Depends(get_db)):
    query = db.query(DistrictData)

    if state_name:
        query = query.filter(DistrictData.state_name.ilike(f"%{state_name.strip()}%"))
    if fin_year and fin_year != "All":
        query = query.filter(DistrictData.fin_year == fin_year)

    data = query.all()
    if not data:
        return {"data": []}

    result = []
    for d in data:
        result.append({
            "district_name": d.district_name,
            "Approved_Labour_Budget": d.Approved_Labour_Budget,
            "Average_Wage_rate_per_day_per_person": d.Average_Wage_rate_per_day_per_person,
            "Average_days_of_employment_provided_per_Household": d.Average_days_of_employment_provided_per_Household,
            "Total_Households_Worked": d.Total_Households_Worked,
            "Total_Individuals_Worked": d.Total_Individuals_Worked,
            "Total_Exp": d.Total_Exp,
            "Wages": d.Wages,
        })
    return {"data": result}
