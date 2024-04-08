# import json
# import pandas as pd
# import pytest
# from typing import List, Dict, Any
# from pathlib import Path
# from _pytest.tmpdir import TempPathFactory
# from src.data_pipeline import ingestion
#
#
# # Sample rental data
# @pytest.fixture
# def rental_data() -> Dict[str, Any]:
#     return {
#         "additionalCostsRaw": "€ 50",
#         "areaSqm": "14 m2",
#         "city": "Rotterdam",
#         "deposit": "€ 500",
#         "descriptionTranslated": "Nice room for rent, accros the Feyenoord stadium in Rotterdam. It has shared "
#                                  "Bathroom and kitchen.",
#         "energyLabel": "Unknown",
#         "furnish": "Unfurnished",
#         "gender": "Mixed",
#         "internet": "Yes",
#         "isRoomActive": True,
#         "kitchen": "Shared",
#         "latitude": 51.8966010000,
#         "living": "None",
#         "longitude": 4.5149930000,
#         "matchCapacity": "1 person",
#         "pets": "No",
#         "postalCode": "3074HN",
#         "postedAgo": "4w",
#         "propertyType": "Room",
#         "availability": "26-06-19 - Indefinite period",
#         "registrationCost": "€ 0",
#         "rent": 500.0,
#         "roommates": 5,
#         "shower": "Shared",
#         "smokingInside": "No",
#         "title": "West-Varkenoordseweg",
#         "toilet": "Shared"
#     }
#
#
# # Sample Airbnb data
# @pytest.fixture
# def airbnb_data() -> Dict[str, Any]:
#     return {
#         "zipcode": "1053",
#         "latitude": 52.37302064,
#         "longitude": 4.868460923,
#         "room_type": "Entire home/apt",
#         "accommodates": 4,
#         "bedrooms": 2.0,
#         "price": 130,
#         "review_scores_value": 100.0
#     }
#
#
# # Test for loading rental data
# def test_load_rentals_data(tmp_path, rental_data):
#     file_path = tmp_path / "rentals.json"
#     with open(file_path, 'w') as f:
#         json.dump([rental_data], f)
#     loaded_data = ingestion.load_rentals_data(file_path)
#     assert loaded_data == [rental_data]
#
#
# # Test for loading Airbnb data
# def test_load_airbnb_data(tmp_path, airbnb_data):
#     file_path = tmp_path / "airbnb.csv"
#     airbnb_df = pd.DataFrame([airbnb_data])
#     airbnb_df.to_csv(file_path, index=False)
#     loaded_data = ingestion.load_airbnb_data(file_path)
#     pd.testing.assert_frame_equal(loaded_data, airbnb_df)