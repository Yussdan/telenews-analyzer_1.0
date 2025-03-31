from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List

class MessageModel(BaseModel):
    telegram_id: int
    channel_id: int
    text: str
    date: datetime
    has_media: bool = False
    processed: bool = False
    tokens: Optional[List[str]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "telegram_id": 1234,
                "channel_id": 1234567890,
                "text": "Sample message text",
                "date": "2023-07-10T15:30:00.000Z",
                "has_media": True,
                "processed": False
            }
        }
