from sqlalchemy import Column, Integer, String, Float
from database import Base

class DistrictData(Base):
    __tablename__ = "district_data"
    id = Column(Integer, primary_key=True, index=True)
    district_name = Column(String)
    state_name = Column(String)
    month = Column(String)
    total_persondays = Column(Float)
