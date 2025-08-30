<h1 style="text-align:center">My Data Engineer Portfolio</h1>

Hi, this is a repository that contains a few projects with my Data Engineer skills. \
My basic tech stack are **<a href="https://www.python.org/">Python</a>, <a href="https://www.postgresql.org/">PostgreSQL</a>, <a href="https://www.mongodb.com/">mongoDB</a>, <a href="https://www.prefect.io/">Prefect Cloud</a>**.
Basic dataset was PostgreSQL: <a href="https://neon.com/postgresql/postgresql-getting-started/postgresql-sample-database"> dvd_rental.tar </a>.

## Project List
| id | name                   | description                                                                                         | status | path                                                                                                                        |
|----|------------------------|-----------------------------------------------------------------------------------------------------|--------|-----------------------------------------------------------------------------------------------------------------------------|
| 1. | Data Migration         | Data migration from localhost to instance on VPS.                                                   | ✅      | [/01_data_migration](https://github.com/Cracowiatschek/DataEngineerPortfolio/tree/master/01_data_migration)                 |
| 2. | Data Mixing            | Daily data transformation, obtaining diffrent results every day while maintaining data consistnecy. | 💤     |                                                                                                                             |
| 3. | Golden Record          | Daily data migration from PostgreSQL to mongoDB and create Golden Record for every customer.        | 🛠️    |                                                                                                                             |
| 4. | Dashboard              | Dashbord with store sales result                                                                    | 💤     |                                                                                                                             |
| 5. | Get data from internet | Daily geting data from API and store them in PostgreSQL.                                            | ✅       | [/05_get_data_from_internet](https://github.com/Cracowiatschek/DataEngineerPortfolio/tree/master/05_get_data_from_internet) |

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

