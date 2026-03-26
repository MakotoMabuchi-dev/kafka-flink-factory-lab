# Superset Local Notes

This directory keeps only the local files needed to connect a manually cloned
Apache Superset environment to the lab's shared Docker network.

It does not vendor the Superset source tree.

Recommended flow:

1. Clone Apache Superset into a separate local directory such as `superset-test/`.
2. Copy [docker-compose.lab.yml](docker-compose.lab.yml)
   into the root of that clone.
3. Create `docker/requirements-local.txt` in the Superset clone and add:

```text
trino
```

4. Start Superset with:

```bash
docker compose -f docker-compose-image-tag.yml -f docker-compose.lab.yml up -d
```

The full step-by-step explanation lives in the main project README.
