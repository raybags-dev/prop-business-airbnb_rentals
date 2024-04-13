from src.middleware import error_handler
from src.utils.loader import worker_emulator
from geopy.geocoders import Nominatim
from logger.logger import initialize_logging, my_log


# Initialize geocoder with a custom user agent (optional)
geolocator = Nominatim(user_agent="my_custom_user_agent")

# Cache to store fetched zipcodes
zipcode_cache = {}


@error_handler.handle_error
def reverse_geocode(latitude, longitude):
    # Use the geocoder to perform reverse geocoding
    location = geolocator.reverse((latitude, longitude), exactly_one=True)
    return location


@error_handler.handle_error
def assign_zipcode(data, clear_cache=False):
    # Clear the cache if requested
    if clear_cache:
        zipcode_cache.clear()
        print(f"Cache cleared...done! - Calling geo reverse search API...")

    worker_emulator('Fetching zipcodes...', True)

    # Iterate over each object in the list
    for obj in data:
        # Check if the object's coordinates are in the cache
        if (obj["latitude"], obj["longitude"]) in zipcode_cache:
            # If so, retrieve the zipcode from the cache
            zipcode = zipcode_cache[(obj["latitude"], obj["longitude"])]
            obj['zipcode'] = zipcode
            continue

        # Check if zipcode is not empty or null before processing
        # if obj['zipcode'] in ('', None, False) or not re.match(r'^\d{4}\s[A-Z]{2}$', obj['zipcode']):
        if obj:
            print(f' -> Fetching zipcode for (lat={obj["latitude"]}, lon={obj["longitude"]})')
            # Perform reverse geocoding
            location = reverse_geocode(obj['latitude'], obj['longitude'])

            # Extract zipcode from address details
            if location and 'postcode' in location.raw['address']:
                zipcode = location.raw['address']['postcode']
                # Assign zipcode to the object and cache (optional)
                obj['zipcode'] = zipcode
                zipcode_cache[(obj["latitude"], obj["longitude"])] = zipcode

                # Append other fields from the API call to the object
                for key, value in location.raw['address'].items():
                    if key != 'postcode':
                        obj[key] = value
            else:
                print(f"Failed to fetch zipcode for ({obj['latitude']}, {obj['longitude']})")

        else:
            print(f"Skipping fetch for object at:\nLatitude: {obj['latitude']} - Longitude: {obj['latitude']}\n")

    worker_emulator('Object ready', False)
    return data

