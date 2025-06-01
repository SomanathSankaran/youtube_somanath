# Databricks notebook source
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional

class User(BaseModel):
    id: int
    name: str = Field(..., min_length=2, max_length=50)
    email: str
    age: Optional[int] = Field(None, ge=18, le=100)  # Optional field with age limit
    hobbies: List[str] = []

# Valid data


# COMMAND ----------

user_data = {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "age": 25,
    "hobbies": ["reading", "gaming"],
    "other":"un-necessary field"
}

user = User(**user_data)
print(user)



# COMMAND ----------

# Invalid data (Triggers ValidationError)
invalid_user_data = {
    "id": 2,
    "name": "B",
    "email": "invalid-email",
    "age": 15,  # Age below 18
}

try:
    invalid_user = User(**invalid_user_data)
except Exception as e:
    print(e)


# COMMAND ----------

