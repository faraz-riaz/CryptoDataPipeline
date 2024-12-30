# tests/test_data_validator.py
import pytest
import pandas as pd
from datetime import datetime
from src.ingestion.data_validator import DataValidator

@pytest.fixture
def validator():
    return DataValidator()

@pytest.fixture
def valid_data():
    return pd.DataFrame({
        'coin_id': ['bitcoin'],
        'price_usd': [50000.0],
        'market_cap_usd': [1e12],
        'volume_24h_usd': [1e10],
        'timestamp': [datetime.utcnow()]
    })

def test_validate_schema(validator, valid_data):
    """Test schema validation with valid data"""
    validated_df = validator.validate_schema(valid_data)
    assert validated_df is not None
    assert all(validated_df.dtypes == valid_data.dtypes)

def test_validate_values(validator, valid_data):
    """Test value validation with valid data"""
    validated_df = validator.validate_values(valid_data)
    assert validated_df is not None

def test_invalid_price(validator, valid_data):
    """Test validation fails with negative price"""
    invalid_data = valid_data.copy()
    invalid_data['price_usd'] = -1000
    
    with pytest.raises(ValueError):
        validator.validate_values(invalid_data)