# data-engineering G12

Data Engineering group project for UT class LTAT.02.007.

# Requirements

Before starting Airflow for the first time, You need to prepare your environment, i.e. create the necessary files,
directories and initialize the database.

On Linux, the quick-start needs to know your host user id and needs to have group id set to `0`. Otherwise the files
created in `dags`, `logs` and `plugins` will be created with root user. You have to make sure to configure them for the
docker-compose:

Run `mkdir -p ./dags ./logs ./plugins`

Run `echo -e "AIRFLOW_UID=$(id -u)" > .env`

# Running

On all operating systems, you need to run database migrations and create the first user account. To do it, run.

`docker-compose up airflow-init`

The account created has the login `airflow` and the password `airflow`.

In order to get Java running in Docker, had to create a Dockerfile following [this guide](https://stackoverflow.com/questions/67268054/how-to-install-java-in-an-airflow-container-using-docker-compose-yaml).

To start all Airflow services in the background: 

`docker-compose up -d --build`

To start monitoring logs of the running Airflow services in terminal:

`docker-compose up`

# Accessing the environment

Once the cluster has started up, you can log in to the web interface and try to run some tasks.

The webserver is available at: `http://localhost:8080`. The default account has the login `airflow` and the
password `airflow`.

# Cleaning up

The following command stops containers and removes containers, networks, volumes, and images created
by `docker-compose up`.

`docker-compose down --volumes --remove-orphans`
