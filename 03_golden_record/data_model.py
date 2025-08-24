from pydantic import BaseModel, Field
from typing import List, Optional,Dict
from datetime import datetime


class SourceCustomer(BaseModel):
    _id: int
    first_name: str
    last_name: str
    is_active: bool
    address: str
    district: str
    city: str
    country: str
    phone: Optional[str]
    email: Optional[str]
    postal_code: Optional[str]
    assistant_first_name: str
    assistant_last_name: str
    assistant_email: str


class SourceRental(BaseModel):
    _id: int
    customer_id: int
    title: str
    category: str
    amount: float
    rental_date: datetime
    return_date: Optional[datetime]
    rental_during: Optional[int]
    store: str


class GoldenCustomer(BaseModel):
    _id: int
    first_name: str
    last_name: str
    is_active: bool
    address: str
    district: str
    city: str
    country: str
    phone: Optional[str]
    email: Optional[str]
    postal_code: Optional[str]
    assistant_first_name: str
    assistant_last_name: str
    assistant_email: str
    most_recent_store: Optional[str]
    last_rental: Optional[str]
    lifetime_value: Optional[float]
    total_rental_counts: int
    average_rental_duration: Optional[float]
    average_rental_payment: Optional[float]
    last_year_rental_counts: Optional[int]
    last_year_payments_sum: Optional[float]
    last_payment: Optional[float]
    most_recent_film_category: Optional[str]
    second_most_recent_film_category: Optional[str]
    third_most_recent_film_category: Optional[str]
    most_recent_film_title: Optional[str]
    most_recent_film_actor: Optional[str]
    most_recent_film_year: Optional[int]
    last_ten_rentals: Optional[List[Dict]]
    last_consolidation_date: datetime
    sources: List[Dict]

