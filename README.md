# data-engineering G12
Data Engineering group project for UT class LTAT.02.007. 

# Requirements

Before starting Airflow for the first time, You need to prepare your environment, i.e. create the necessary files, directories and initialize the database.

On Linux, the quick-start needs to know your host user id and needs to have group id set to `0`. Otherwise the files created in `dags`, `logs` and `plugins` will be created with root user. You have to make sure to configure them for the docker-compose:

Run `mkdir -p ./dags ./logs ./plugins`

Run `echo -e "AIRFLOW_UID=$(id -u)" > .env`

# Running

On all operating systems, you need to run database migrations and create the first user account. To do it, run.

`docker-compose up airflow-init`

The account created has the login `airflow` and the password `airflow`.

To start all Airflow services: 

`docker-compose up`

To check whether all containers started up - `docker-compose up`


# Cleaning up

The following command stops containers and removes containers, networks, volumes, and images created by `docker-compose up`. 

`docker-compose down --volumes --remove-orphans`
