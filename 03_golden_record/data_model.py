from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, confloat, EmailStr


class GoldenRental(BaseModel):
    _id = int
    title: str
    category: str
    amount: float
    rental_date: datetime
    return_date: Optional[datetime]
    rental_duration: int
    is_completed: bool
    is_overdue: bool
    store: str


class Source(BaseModel):
    _id: int
    path: str
    fields: List[str]
    last_refreshed: datetime


class GoldenCustomer(BaseModel):
    _id: int
    first_name: str
    last_name: str
    is_active: bool
    full_address: str
    address: str
    district: Optional[str]
    city: str
    country: str
    latitude: confloat(ge=-90, le=90) = 0.0
    longitude: confloat(ge=-180, le=180) = 0.0
    phone: Optional[str]
    email: Optional[EmailStr]
    postal_code: Optional[str]
    assistant_name: str
    assistant_email: str
    overdue_score: int
    most_recent_store: Optional[str]
    last_rental_film: Optional[str]
    last_rental_date: Optional[str]
    lifetime_value: float = 0.00
    total_rental_count: int = 0
    average_rental_duration: float = 0.00
    average_rental_payment: float = 0.00
    average_film_duration: float = 0.00
    last_year_rental_count: int = 0
    last_year_payments_sum: float = 0.00
    last_payment: float = 0.00
    most_recent_film_category: Optional[str]
    second_most_recent_film_category: Optional[str]
    third_most_recent_film_category: Optional[str]
    most_recent_film_title: Optional[str]
    most_recent_film_actor: Optional[str]
    most_recent_film_year: Optional[str]
    last_ten_rentals: Optional[List[GoldenRental]]
    last_consolidation_date: datetime
    sources: List[Source]

