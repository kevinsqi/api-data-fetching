import asyncio
import math
from datetime import datetime, timezone
from typing import List

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Hardcoded list of 1000 meter IDs
METERS = [{"meter_id": f"1020{100000 + i:06d}"} for i in range(1000)]

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

    Each meter has a distinct pattern based on its ID:
    - Different peak hours (shifted by meter bucket)
    - Different magnitude multipliers
    - Different base loads
    """
    # Extract meter number for deterministic variation
    meter_num = int(meter_id[-6:])

    # Create distinct buckets for different meter behaviors
    magnitude_bucket = meter_num % 10  # 0-9: affects overall scale
    time_shift_bucket = (meter_num // 10) % 10  # 0-9: shifts peak hours
    pattern_bucket = (meter_num // 100) % 10  # 0-9: morning vs evening person

    hour = dt.hour + dt.minute / 60.0

    # Shift peak hours by -4 to +5 hours based on meter
    hour_shifted = (hour - time_shift_bucket + 4) % 24

    # Base load varies 3-12 kWh based on meter
    base_load = 3.0 + magnitude_bucket * 1.0

    # Magnitude multiplier 0.5x to 1.5x
    magnitude = 0.5 + (magnitude_bucket / 9.0)

    # Daily pattern using sine wave
    daily_phase = (hour_shifted - 14) * (2 * math.pi / 24)
    daily_variation = -math.cos(daily_phase)

    # Morning vs evening person based on pattern_bucket
    morning_weight = (10 - pattern_bucket) / 10.0  # 1.0 to 0.1
    evening_weight = pattern_bucket / 10.0  # 0.0 to 0.9

    morning_spike = math.exp(-((hour_shifted - 7.5) ** 2) / 2) * 5.0 * morning_weight
    evening_spike = math.exp(-((hour_shifted - 19) ** 2) / 2) * 6.0 * evening_weight

    # Combine components with magnitude scaling
    usage = base_load + ((daily_variation + 1) * 8.0 + morning_spike + evening_spike) * magnitude

    # Add minute-level variation (deterministic per meter)
    minute_variation = math.sin(dt.minute * 0.1 + meter_num * 0.1) * 0.5
    usage += minute_variation

    # Ensure non-negative and round to 2 decimal places
    return round(max(0.0, usage), 2)


# Build a set of valid meter IDs for fast lookup
VALID_METER_IDS = {m["meter_id"] for m in METERS}


@app.get("/meters", response_model=List[Meter])
@limiter.limit("20/second")
async def get_meters(request: Request):
    """Return list of all 1000 meters."""
    await asyncio.sleep(0.1)
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

    await asyncio.sleep(0.1)

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
