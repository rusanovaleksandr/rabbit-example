from pydantic import BaseModel


class RaceEvent(BaseModel):
    race: str
    driver: str
    position: int
    lap: int
    timestamp: str
