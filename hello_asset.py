import pandas as pd
from pandas import DataFrame

from dagster import AssetKey, SourceAsset, asset


sfo_q2_weather_sample = SourceAsset(
    key=AssetKey("sfo_q2_weather_sample"),
    description="Weather samples, taken every five minutes at SFO",
    metadata={"format": "csv"},
)

