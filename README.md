# cord-19-elasticsearch-indexing
Code for indexing cord-19 dataset into elastic search. Includes systemd service to update it periodically and docker files to create elastic search container.

## Setup
The service works with a systemd timer that runs every Sunday. It needs `docker-compose` command working, in the case of installing it with pip, it will probably be under `$HOME/.local/bin`, currently,
the service file has this path hardcoded (couldn't get variable expansion working at the time of writing), to install it, just adjust this path and issue `make install` within the service folder.
