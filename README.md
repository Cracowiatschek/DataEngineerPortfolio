<h1 style="text-align:center">My Data Engineer Portfolio</h1>

Hi, this is a repository that contains a few projects with my Data Engineer skills. \
My basic tech stack are **<a href="https://www.python.org/">Python</a>, <a href="https://www.postgresql.org/">PostgreSQL</a>, <a href="https://www.mongodb.com/">mongoDB</a>, <a href="https://www.prefect.io/">Prefect Cloud</a>**.
Basic dataset was PostgreSQL: <a href="https://neon.com/postgresql/postgresql-getting-started/postgresql-sample-database"> dvd_rental.tar </a>.

## Project List
| id | name                   | description                                                                                         | status | path                                                                                                                        |
|----|------------------------|-----------------------------------------------------------------------------------------------------|--------|-----------------------------------------------------------------------------------------------------------------------------|
| 1. | Data Migration         | Data migration from localhost to instance on VPS.                                                   | ✅      | [/01_data_migration](https://github.com/Cracowiatschek/DataEngineerPortfolio/tree/master/01_data_migration)                 |
| 2. | Data Mixing            | Daily data transformation, obtaining diffrent results every day while maintaining data consistnecy. | 💤     |                                                                                                                             |
| 3. | Golden Record          | Daily data migration from PostgreSQL to mongoDB and create Golden Record for every customer.        | ✅️     | [/03_golden_record](https://github.com/Cracowiatschek/DataEngineerPortfolio/tree/master/03_golden_record)                   |
| 4. | Categorize pipeline    | Pipeline to customer categorize in Golden Record.                                                   | 🛠️    |                                                                                                                             |
| 5. | Get data from internet | Daily getting data from API and store them in PostgreSQL.                                           | ✅      | [/05_get_data_from_internet](https://github.com/Cracowiatschek/DataEngineerPortfolio/tree/master/05_get_data_from_internet) |

## 1. Data Migration
This is initializing my all projects. My target was having access to sample dataset from any devices to training for me and my wife. \
When I was creating structures I came an idea for this portfolio. \
So, in this project I was created copy of schema dvd_rental and migrated data from localhost to VPS with Prefect Cloud.  
Data was copied table by table, if I got some error, process was interrupt.

**Tech stack:**
* Python <img src="https://s3.dualstack.us-east-2.amazonaws.com/pythondotorg-assets/media/community/logos/python-logo-only.png" height="15">
* PostgreSQL <img src="https://www.postgresql.org/media/img/about/press/elephant.png" height="15">
* Prefect Cloud <img src="https://www.prefect.io/_next/image?url=%2F_next%2Fstatic%2Fmedia%2Fprefect.999a8755.svg&w=32&q=75" height="15">
### 1.1. Create similar data schema in VPS PostgreSQL instance.
![](/assets/dvd_rental.png)
### 1.2. Create load data config file.
This is json with list of tables. 

#### **Structure:** 
* **name**: table name,
* **schema_in**: schema from localhost,
* **schema_out**: schema from VPS,
* **depends_on**: list of table dependencies needed to create a migration queue.

### 1.3. Create migration flow.
#### **Flow has three steps:**
1. load configuration file,
2. create migration queue,
3. table-by-table migration according to the queue.

#### 1.3.1. Load configuration file *@task*
File was loaded from json as list of dict.

#### 1.3.2. Create migration queue *@task*
A queue was created from the configuration list so that tables without dependencies were migrated first, and then, as dependencies were added, the remaining tables were migrated.

#### 1.3.3. Migration *@task*
The migration consisted of downloading the table contents from localhost and then inserting the contents according to the list of columns retrieved from the metadata in batch form.
In case of an error, the table migration was interrupted and a rollback was performed for the given table.

#### 1.3.4. Run Flow with Prefect *@flow*

## 3. Golden Record
For this project I created three materialized views to prepare data (```customers_mv```➡️simple customer attributes without joins, ```customer_aggr_mv```➡️aggregations for every customer, ```last_rentals_mv```➡️last ten rentals for every customer). \
Project has two steps:
1. configuration (export all files/variables to Prefect Cloud) 
2. refresh materialized views
3. create (_upsert_) every customer as Golden Record to NoSQL DB (MongoDB) 

On the platform Prefect Cloud I set rule if flow with refresh views has Failed or Crashed status, next flow with Golden Record upsert waiting for manual restart earlier process.

![](/assets/03_flows.png)

**Tech stack:**
* Python <img src="https://s3.dualstack.us-east-2.amazonaws.com/pythondotorg-assets/media/community/logos/python-logo-only.png" height="15">
* PostgreSQL <img src="https://www.postgresql.org/media/img/about/press/elephant.png" height="15">
* MongoDB <img src="https://cdn.worldvectorlogo.com/logos/mongodb-icon-1.svg" height="15">
* Prefect Cloud <img src="https://www.prefect.io/_next/image?url=%2F_next%2Fstatic%2Fmedia%2Fprefect.999a8755.svg&w=32&q=75" height="15">
* GitHub <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Octicons-mark-github.svg/960px-Octicons-mark-github.svg.png?20180806170715" height="15">

**Materialized views:** 
#### ```customer_mv``` 

| id  | column name         | dtype       | description                                      | source                                |
|-----|---------------------|-------------|--------------------------------------------------|---------------------------------------|
| 1.  | customer_id         | int         | customer id                                      | ```dvd_rental.customer.customer_id``` |
| 2.  | first_name          | varchar(45) | customer first name                              | ```dvd_rental.customer.first_name```  |
| 3.  | last_name           | varchar(45) | customer last name                               | ```dvd_rental.customer.last_name```   |
| 4.  | is_active           | bool        | is active indicator                              | ```dvd_rental.customer.activebool```  |
| 5.  | address             | varchar(50) | home number and street                           | ```dvd_rental.address.address```      |
| 6.  | district            | varchar(20) | district                                         | ```dvd_rental.address.district```     |
| 7.  | city                | varchar(50) | city                                             | ```dvd_rental.city.city```            |
| 8.  | longitude           | float       | longitude of city center                         | ```dvd_rental.city.longitude```       |
| 9.  | latitude            | float       | latitude of city center                          | ```dvd_rental.city.latitude```        |
| 10. | country             | varchar(50) | country                                          | ```dvd_rental.country.country```      |
| 11. | full_address        | text        | full address = address, city, country (district) |                                       |
| 12. | phone               | varchar(20) | phone number                                     | ```dvd_rental.address.phone```        |
| 13. | email               | varchar(50) | email                                            | ```dvd_rental.customer.email```       |
| 14. | postal_code         | varchar(10) | postal code                                      | ```dvd_rental.address.postal_code```  |
| 15. | assistant_name      | varchar(45) | shop assistant first name                        | ```dvd_rental.staff.first_name```     |
| 16. | assistant_last_name | varchar(45) | shop assistant last name                         | ```dvd_rental.staff.last_name```      |
| 17. | assistant_email     | varchar(45) | shop assistant email                             | ```dvd_rental.staff.email```          |
| 18. | last_refresh_date   | timestamp   | timestamp of view refresh                        |                                       |

#### ```customer_aggr_mv```
| id  | column name                 | dtype     | description                                       | source                                                                          |
|-----|-----------------------------|-----------|---------------------------------------------------|---------------------------------------------------------------------------------|
| 1.  | customer_id                 | int       | customer id                                       | ```dvd_rental.customer.customer_id```                                           |
| 2.  | overdue_score               | int       | percentage of late returns                        | ```dvd_rental.rental.return_date-rental_date>dvd_rental.film.rental_duration``` |
| 3.  | most_recent_store           | text      | store where customer has the most rentals         |                                                                                 |
| 4.  | last_rental_film            | text      | last rental film title                            |                                                                                 |
| 5.  | last_rental_date            | timestamp | last rental date                                  |                                                                                 |
| 6.  | lifetime_value              | numeric   | sum of all rentals amount                         |                                                                                 |
| 7.  | total_rental_count          | int       | count of all rentals                              |                                                                                 |
| 8.  | average_rental_duration     | numeric   | average from all rentals durations                |                                                                                 |
| 9.  | average_rental_payment      | numeric   | average from all rentals payments                 |                                                                                 |
| 10. | average_film_duration       | numeric   | average from all rentals film duration            |                                                                                 |
| 11. | last_year_rental_count      | int       | count of rentals from last year                   |                                                                                 |
| 12. | last_year_payments_sum      | numeric   | sum of rentals amount from last year              |                                                                                 |
| 13. | last_payment                | numeric   | amount of last payment                            |                                                                                 |
| 14. | most_recent_category        | text      | most recent film category from all rentals        |                                                                                 |
| 15. | second_most_recent_category | text      | second most recent film category from all rentals |                                                                                 |
| 16. | third_most_recent_category  | text      | third most recent film category from all rentals  |                                                                                 |
| 17. | most_recent_film_title      | text      | most recent film title from all rentals           |                                                                                 |
| 18. | most_recent_film_actor      | text      | most recent film actor from all rentals           |                                                                                 |
| 19. | most_recent_film_year       | text      | most recent film year from all rentals            |                                                                                 |
| 20. | last_refresh_date           | timestamp | timestamp of view refresh                         |                                                                                 |

#### ```last_rentals_mv```
| id  | column name        | dtype        | description                                                                         | source                                                                                                                                                  |
|-----|--------------------|--------------|-------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1.  | rental_id          | int          | rental id                                                                           | ```dvd_rental.rental.rental_id```                                                                                                                       |
| 2.  | customer_id        | int          | customer id                                                                         | ```dvd_rental.rental.customer_id```                                                                                                                     |
| 3.  | title              | varchar(255) | film title                                                                          | ```dvd_rental.film.title```                                                                                                                             |
| 4.  | category           | varchar(25)  | film category                                                                       | ```dvd_rental.category.name```                                                                                                                          |
| 5.  | amount             | numeric      | payment                                                                             | ```coalesce(dvd_rental.rental.amount, dvd_rental.film.replacement_cost)```                                                                              |
| 6.  | rental_date        | timestamp    | rental date                                                                         | ```dvd_rental.rental.rental_date```                                                                                                                     |
| 7.  | return_date        | timestamp    | return date                                                                         | ```dvd_rental.rental.return_date```                                                                                                                     |
| 8.  | rental_duration    | int          | rental duration in days                                                             | ```coalesce(dvd_rental.rental.return_date, now())-dvd_rental.rental.rental_date```                                                                      |
| 9.  | is_completed       | bool         | true if film was returned                                                           | ```case when dvd_rental.rental.return_date is not null then true else false end```                                                                      |
| 10. | is_overdue         | bool         | true if return date (or now if return date is empty) was after film rental duration | ```case when coalesce(dvd_rental.rental.return_date, now())-dvd_rental.rental.rental_date > dvd_rental.film.rental_duration then true else false end``` |
| 11. | store              | text         | store where film was rental                                                         | ```address, city, country (district)```                                                                                                                 |
| 12. | last_refresh_date  | timestamp    | timestamp of view refresh                                                           |                                                                                                                                                         |


## 3.1. Refresh materialized views  *@flow*
This flow has few task: first for make queue from Prefect Cloud Block, second to getting last refresh date for all views and third to refresh view.
If no all views are refreshed, flow try rerun 3 times.

### 3.1.1 Make queue from JsonConfig block *@task*
At flow is load config from cloud, object is json with structure of array of:
```json
[
  {
    "schema": "v_dvd_rental",
    "materialized_view": "customers_mv",
    "sources": [
      {
        "schema": "dvd_rental",
        "table": "customer"
      }, {
        "schema": "dvd_rental",
        "table": "address"
      }, {
        "schema": "dvd_rental",
        "table": "city"
      }, {
        "schema": "dvd_rental",
        "table": "country"
      }, {
        "schema": "dvd_rental",
        "table": "store"
      }, {
        "schema": "dvd_rental",
        "table": "staff"
      }
    ]
  }
]
```
And in the next step object is load and make queue of ```namedtuple("MaterializedView", ["schema", "view_name", "sources"])```, where sources is list of ```namedtuple("Source", ["schema", "table_name"])```.
Sources is important to materialize Assets in Prefect Cloud, thanks to this, I have a full overview of overload situations on the cloud graph.

### 3.1.2. While loop with tasks:

#### 3.1.2.1. Check last refreshed data *@task*
Task get timestamp and save it in dict with two lists of timestamps ```before``` and ```after```, at this time timestamp go to before list.

#### 3.1.2.2. Try refresh view *@task*
Task try refresh materialized view from queue.

#### 3.1.2.3. Check last refreshed data *@task*
Like first checking, but at this time timestamp go to after list.

#### 3.1.2.4. Check refreshed timestamps *@task*
If some timestamps is in both lists, raise error NotAllViewsRefreshed, and retry flow (limit=3 times with 120 seconds delay).

### 3.1.3. Deploy.
Flow is deployed in Prefect Cloud with hobby workpool, and code is getting from GitHub repository.

## 3.2. Upsert Golden Record *@flow*
This flow has another a few task: first to get data from PostgreSQL (from refreshed materialized views),  
