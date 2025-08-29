select cm.customer_id _id, cm.first_name, cm.last_name, cm.is_active,
	cm.full_address, cm.address, cm.district, cm.city, cm.country,
	cm.phone, cm.email, cm.postal_code, 
	cm.assistant_name||' '||cm.assistant_last_name assistant_name,
	cm.assistant_email, 
	cam.overdue_score, cam.most_recent_store, cam.last_rental_film,
	cam.last_rental_date, cam.lifetime_value, cam.total_rental_count,
	cam.average_rental_duration, cam.average_rental_payment, cam.average_film_duration,
	cam.last_year_rental_count, cam.last_year_payments_sum, cam.last_payment,
	cam.most_recent_category, cam.second_most_recent_category, cam.third_most_recent_category,
	cam.most_recent_film_title, cam.most_recent_film_actor, cam.most_recent_film_year,
	json_agg(
		json_build_object(
			'_id', lrm.rental_id,
			'title', lrm.title,
			'category', lrm.category,
			'amount', lrm.amount,
			'rental_date', lrm.rental_date,
			'return_date', lrm.return_date,
			'rental_duration', lrm.rental_duration,
			'is_completed', lrm.is_completed,
			'is_overdue', lrm.is_overdue,
			'store', lrm.store 
		)
	)
from v_dvd_rental.customers_mv cm 
left join v_dvd_rental.customer_aggr_mv cam on cm.customer_id = cam.customer_id
left join v_dvd_rental.last_rentals_mv lrm on cm.customer_id = lrm.customer_id 
group by cm.customer_id, cm.first_name, cm.last_name, cm.is_active,
cm.full_address, cm.address, cm.district, cm.city, cm.country,
cm.phone, cm.email, cm.postal_code, 
cm.assistant_name ,cm.assistant_last_name,
cm.assistant_email, 
cam.overdue_score, cam.most_recent_store, cam.last_rental_film,
cam.last_rental_date, cam.lifetime_value, cam.total_rental_count,
cam.average_rental_duration, cam.average_rental_payment, cam.average_film_duration,
cam.last_year_rental_count, cam.last_year_payments_sum, cam.last_payment,
cam.most_recent_category, cam.second_most_recent_category, cam.third_most_recent_category,
cam.most_recent_film_title, cam.most_recent_film_actor, cam.most_recent_film_year