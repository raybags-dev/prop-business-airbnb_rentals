import pytest
from unittest.mock import patch, Mock
from src.utils.geoLocator import reverse_geocode, assign_zipcode


# <----------- MOCK GEOLOCATOR.REVERSE METHOD TESTS ---------->
@patch('src.utils.geoLocator.geolocator.reverse')
def test_reverse_geocode(mock_reverse):
    mock_location = Mock()
    mock_location.raw = {'address': {'postcode': '12345'}}
    mock_reverse.return_value = mock_location

    latitude, longitude = 40.7128, -74.0060
    location = reverse_geocode(latitude, longitude)

    assert location == mock_location


# <----------- MOCK WORK EMULATOR && GEOLOCATOR.REVERSE METHOD TESTS ---------->
@patch('src.utils.geoLocator.worker_emulator')
@patch('src.utils.geoLocator.geolocator.reverse')
def test_assign_zipcode(mock_reverse, mock_worker_emulator):
    mock_location = Mock()
    mock_location.raw = {'address': {'postcode': '12345'}}
    mock_reverse.return_value = mock_location

    data = [
        {'latitude': 40.7128, 'longitude': -74.0060},
        {'latitude': 34.0522, 'longitude': -118.2437}
    ]

    assigned_data = assign_zipcode(data)

    assert assigned_data == [
        {'latitude': 40.7128, 'longitude': -74.0060, 'zipcode': '12345'},
        {'latitude': 34.0522, 'longitude': -118.2437, 'zipcode': '12345'}
    ]

