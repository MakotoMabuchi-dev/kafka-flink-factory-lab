# Superset Local Stack

This directory contains a lightweight local Superset stack for the lab.

It uses:

- `docker-compose.yml` to run a single Superset container on `http://localhost:8088`
  - if `8088` is busy, the CLI can republish it on the next free port
- `Dockerfile` to install the `trino` SQLAlchemy driver on top of `apache/superset:6.0.0`
- `scripts/start_lab_superset.sh` to initialize Superset and upsert the Trino connection
- `pythonpath/superset_config.py` for local metadata and cache settings

Default login:

- username: `admin`
- password: `admin`

Default database connection created on startup:

- name: `trino_iceberg_lab`
- URI: `trino://trino@trino:8080/iceberg`

Runtime metadata is stored in `superset-local/data/`.
