import math
from datetime import datetime, timezone
from typing import List

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from meters_data import METERS

# Rate limiter setup - 20 requests per second globally
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Meter Usage API")
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "Rate limit exceeded. Maximum 20 requests per second."},
    )


class Meter(BaseModel):
    meter_id: str


class UsageReading(BaseModel):
    timestamp: str
    value: float


def get_usage_value_for_time(dt: datetime, meter_id: str) -> float:
    """
    Deterministically generate usage value based on timestamp and meter_id.

    Usage pattern:
    - Base load: ~5 kWh (always-on appliances)
    - Peak during day (8am-8pm): up to ~25 kWh
    - Low at night (12am-6am): ~3-8 kWh
    - Smooth transitions using sine waves

    Meter ID adds slight variation so different meters have different patterns.
    """
    # Extract meter number for deterministic variation
    meter_num = int(meter_id[-6:])
    meter_offset = (meter_num % 100) / 100.0  # 0.0 to 0.99

    hour = dt.hour + dt.minute / 60.0

    # Base load (always-on: fridge, etc.)
    base_load = 5.0 + meter_offset * 2.0

    # Daily pattern using sine wave
    # Peak at 2pm (hour 14), trough at 2am (hour 2)
    daily_phase = (hour - 14) * (2 * math.pi / 24)
    daily_variation = -math.cos(daily_phase)  # -1 to 1, peaks at hour 14

    # Morning spike (people waking up, 6-9am)
    morning_spike = math.exp(-((hour - 7.5) ** 2) / 2) * 3.0

    # Evening spike (cooking, TV, 6-9pm)
    evening_spike = math.exp(-((hour - 19) ** 2) / 2) * 4.0

    # Combine components
    usage = base_load + (daily_variation + 1) * 8.0 + morning_spike + evening_spike

    # Add minute-level variation (small, deterministic)
    minute_variation = math.sin(dt.minute * 0.1 + meter_num * 0.01) * 0.5
    usage += minute_variation

    # Ensure non-negative and round to 2 decimal places
    return round(max(0.0, usage), 2)


# Build a set of valid meter IDs for fast lookup
VALID_METER_IDS = {m["meter_id"] for m in METERS}


@app.get("/meters", response_model=List[Meter])
@limiter.limit("20/second")
async def get_meters(request: Request):
    """Return list of all 1000 meters."""
    return METERS


@app.get("/meter-usage", response_model=List[UsageReading])
@limiter.limit("20/second")
async def get_meter_usage(
    request: Request,
    meter_id: str = Query(..., description="The meter ID to query"),
    start_time: str = Query(..., description="Start time in ISO format (e.g., 2025-01-01T00:00:00Z)"),
    end_time: str = Query(..., description="End time in ISO format (e.g., 2025-01-01T01:00:00Z)"),
):
    """
    Return minute-level usage data for a meter within the specified time range.

    Returns data points at 1-minute intervals from start_time up to (but not including) end_time.
    """
    # Validate meter_id
    if meter_id not in VALID_METER_IDS:
        raise HTTPException(status_code=404, detail=f"Meter {meter_id} not found")

    # Parse timestamps
    try:
        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp format: {e}")

    if start_dt >= end_dt:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")

    # Limit range to prevent abuse (max 24 hours = 1440 minutes)
    total_minutes = int((end_dt - start_dt).total_seconds() / 60)
    if total_minutes > 1440:
        raise HTTPException(
            status_code=400,
            detail="Time range too large. Maximum 24 hours (1440 minutes) allowed.",
        )

    # Generate minute-level readings
    readings = []
    current = start_dt
    from datetime import timedelta

    while current < end_dt:
        value = get_usage_value_for_time(current, meter_id)
        readings.append({
            "timestamp": current.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "value": value,
        })
        current += timedelta(minutes=1)

    return readings


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7777)
