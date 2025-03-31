from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional

class TrendModel(BaseModel):
    id: Optional[int] = None
    cluster_id: int
    keywords: List[str]
    top_messages: List[int]  # List of telegram_ids
    channels: List[int]  # List of channel_ids
    message_count: int
    first_seen: datetime
    last_seen: datetime
    growth_rate: float  # messages per hour
    
    class Config:
        schema_extra = {
            "example": {
                "id": 1,
                "cluster_id": 5,
                "keywords": ["политика", "заявление", "президент"],
                "top_messages": [1234, 5678, 9012],
                "channels": [11111, 22222],
                "message_count": 15,
                "first_seen": "2023-07-10T10:00:00.000Z",
                "last_seen": "2023-07-10T18:30:00.000Z",
                "growth_rate": 1.87
            }
        }
