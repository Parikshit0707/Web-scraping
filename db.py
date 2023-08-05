from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List

# Replace the database URL with your MySQL connection string
DATABASE_URL = "mysql+mysqlconnector://parikshit:12345678@localhost/BSE"

# Create the FastAPI app
app = FastAPI()

# Connect to the database
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define your data model


class Deal(Base):
    __tablename__ = "Market_Data"
    id = Column(Integer, primary_key=True, index=True)
    Deal_Date = Column(String)
    Security_Code = Column(String, index=True)
    Security_Name = Column(String)
    Client_Name = Column(String)
    Deal_Type = Column(String)
    Quantity = Column(String)
    Price = Column(String)

# Pydantic model for the response data


class DealResponse(BaseModel):
    id: int
    Deal_Date: str
    Security_Code: str
    Security_Name: str
    Client_Name: str
    Deal_Type: str
    Quantity: str
    Price: str

# API endpoints for CRUD operations


class DealCreate(BaseModel):
    Deal_Date: str
    Security_Code: str
    Security_Name: str
    Client_Name: str
    Deal_Type: str
    Quantity: str
    Price: str


class DealUpdate(DealCreate):
    pass


class UpdateFields(BaseModel):
    Deal_Date: str = None
    Security_Code: str = None
    Security_Name: str = None
    Client_Name: str = None
    Deal_Type: str = None
    Quantity: int = None
    Price: float = None


@app.post("/deals/", response_model=DealResponse)
def create_deal(deal: DealCreate):
    db = SessionLocal()
    db_deal = Deal(**deal.dict())
    db.add(db_deal)
    db.commit()
    db.refresh(db_deal)
    return db_deal


@app.get("/deals/{deal_id}", response_model=DealResponse)
def read_deal(deal_id: int):
    db = SessionLocal()
    deal = db.query(Deal).filter(Deal.id == deal_id).first()
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    return deal


@app.get("/deals/", response_model=List[DealResponse])
def read_all_deals():
    db = SessionLocal()
    deals = db.query(Deal).all()
    return deals


@app.patch("/deals/{item_id}")
def update_item(item_id: int, updated_fields: UpdateFields):
    db = SessionLocal()
    db_item = db.query(Deal).filter(Deal.id == item_id).first()

    if db_item is None:
        raise HTTPException(status_code=404, detail="Item not found")

    # Update only the specified fields based on item_id
    for attr, value in updated_fields.dict().items():
        if value is not None:
            setattr(db_item, attr, value)

    db.commit()
    db.refresh(db_item)
    return db_item


@app.delete("/deals/{deal_id}", response_model=dict)
def delete_deal(deal_id: int):
    db = SessionLocal()
    deal = db.query(Deal).filter(Deal.id == deal_id).first()
    if deal is None:
        raise HTTPException(status_code=404, detail="Deal not found")
    db.delete(deal)
    db.commit()
    return {"message": "Deal deleted"}
