## Run postgres on docker

### Preconditions
Have docker installed in your machine.

### Run command

> docker run --name ojp-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres


#### docker run 
Tells Docker to run a new container.

#### --name ojp-postgres	
Assigns the name ojp-postgres to the container (makes it easier to manage and reference).

#### -e POSTGRES_PASSWORD=mysecretpassword	
Sets an environment variable (POSTGRES_PASSWORD) inside the container, required by the postgres image for DB setup.

#### -d	
Runs the container in detached mode (in the background).

#### -p 5432:5432	
Maps port 5432 of your host to port 5432 of the container (PostgreSQL’s default port), allowing local access.

#### postgres	
Specifies the Docker image to use (in this case, the official PostgreSQL image from Docker Hub).