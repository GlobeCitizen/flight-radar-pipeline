API_LIMIT = 1500

def divide_zone(zone_data: dict) -> dict:
    """
    Divide a zone into 4 subzones.

    :param zone_data: dict
    :return: dict
    """
    min_lat, max_lat, min_lon, max_lon = zone_data['br_y'], zone_data['tl_y'], zone_data['tl_x'], zone_data['br_x']
    # Calculate the middle point of the zone
    mid_lat, mid_lon = (min_lat + max_lat) / 2, (min_lon + max_lon) / 2
    return {
        'top_left': {'tl_y': max_lat, 'tl_x': min_lon, 'br_y': mid_lat, 'br_x': mid_lon},
        'top_right': {'tl_y': max_lat, 'tl_x': mid_lon, 'br_y': mid_lat, 'br_x': max_lon},
        'bottom_left': {'tl_y': mid_lat, 'tl_x': min_lon, 'br_y': min_lat, 'br_x': mid_lon},
        'bottom_right': {'tl_y': mid_lat, 'tl_x': mid_lon, 'br_y': min_lat, 'br_x': max_lon},
    }


def get_flights(api, zone_data: dict, zone_name: str='') -> dict:
    flights = {}

    if 'subzones' in zone_data:
        for subzone_name, subzone_data in zone_data['subzones'].items():
            flights.update(get_flights(api, subzone_data, f'{zone_name} - {subzone_name}'))
    else:
        bounds = api.get_bounds(zone_data)
        flights_data = api.get_flights(bounds=bounds)
        if len(flights_data) == API_LIMIT:
            subzones = divide_zone(zone_data)
            for subzone_name, subzone_data in subzones.items():
                flights.update(get_flights(api, subzone_data, f'{zone_name} - {subzone_name}'))
        else:
            flights[zone_name] = flights_data

    return flights


def get_all_flights(api) -> list:
    zones = api.get_zones()
    flights = {}
    for zone_name, zone_data in zones.items():
        flights.update(get_flights(api, zone_data, zone_name))

    all_flights = [flight for flights_in_zone in flights.values() for flight in flights_in_zone]

    return all_flights

def get_all_airlines(api) -> list:
    return api.get_airlines()

def get_all_airports(api) -> list:
    return api.get_airports()