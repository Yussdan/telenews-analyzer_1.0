from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from ..models.trend import TrendModel
from ..storage.postgres import PostgresStorage

router = APIRouter()
postgres = None  # Will be set from main.py

@router.get("/trends", response_model=List[TrendModel])
async def get_trends(limit: int = Query(20, ge=1, le=100)):
    """Get recent trends"""
    if not postgres:
        raise HTTPException(status_code=503, detail="Database not available")
    
    trends = await postgres.get_trends(limit=limit)
    return trends

@router.get("/trends/{trend_id}", response_model=TrendModel)
async def get_trend(trend_id: int):
    """Get a specific trend by ID"""
    if not postgres:
        raise HTTPException(status_code=503, detail="Database not available")
    
    trend = await postgres.get_trend_by_id(trend_id)
    if not trend:
        raise HTTPException(status_code=404, detail="Trend not found")
    
    return trend

@router.get("/trends/keywords/{keyword}", response_model=List[TrendModel])
async def search_trends_by_keyword(keyword: str, limit: int = Query(10, ge=1, le=50)):
    """Search trends by keyword"""
    if not postgres:
        raise HTTPException(status_code=503, detail="Database not available")
    
    trends = await postgres.search_trends_by_keyword(keyword, limit=limit)
    return trends
