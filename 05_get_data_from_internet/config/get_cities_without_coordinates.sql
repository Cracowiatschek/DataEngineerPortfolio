select c.city_id, c.city, ctr.country 
from dvd_rental.city c 
left join dvd_rental.country ctr on c.country_id = ctr.country_id 
where is_set = false;