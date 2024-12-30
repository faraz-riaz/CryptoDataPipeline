# tests/test_coingecko_client.py
import pytest
import pandas as pd
from datetime import datetime
from src.ingestion.coingecko_client import CoinGeckoClient
from src.config import Config

@pytest.fixture
def client():
    return CoinGeckoClient()

def test_get_current_prices(client):
    """Test fetching current prices for a single coin"""
    df = client.get_current_prices(['bitcoin'])
    
    # Check basic DataFrame properties
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == ['coin_id', 'price_usd', 'market_cap_usd', 'volume_24h_usd', 'timestamp']
    
    # Check data types
    assert df['coin_id'].dtype == 'object'
    assert df['price_usd'].dtype == 'float64'
    assert df['market_cap_usd'].dtype == 'float64'
    assert df['volume_24h_usd'].dtype == 'float64'
    
    # Check values
    assert df['coin_id'].iloc[0] == 'bitcoin'
    assert df['price_usd'].iloc[0] > 0
    assert df['market_cap_usd'].iloc[0] > 0
    assert df['volume_24h_usd'].iloc[0] > 0