UPDATE dvd_rental.city AS c
SET longitude = t.longitude,
    latitude  = t.latitude,
    is_set = true
FROM tmp_cities t
WHERE c.city_id = t.city_id;