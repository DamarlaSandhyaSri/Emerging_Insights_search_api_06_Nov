from pydantic import BaseModel


class Sample(BaseModel):
    id: str
    name: str
    age: str
