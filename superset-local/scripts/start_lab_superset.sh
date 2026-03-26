#!/usr/bin/env bash
set -euo pipefail

ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_FIRSTNAME="${ADMIN_FIRSTNAME:-Lab}"
ADMIN_LASTNAME="${ADMIN_LASTNAME:-Admin}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@example.com}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"
TRINO_DATABASE_NAME="${SUPERSET_TRINO_DATABASE_NAME:-trino_iceberg_lab}"
TRINO_SQLALCHEMY_URI="${SUPERSET_TRINO_SQLALCHEMY_URI:-trino://trino@trino:8080/iceberg}"

echo_step() {
  cat <<EOF
######################################################################
$1
######################################################################
EOF
}

create_admin_user() {
  set +e
  output=$(
    superset fab create-admin \
      --username "${ADMIN_USERNAME}" \
      --firstname "${ADMIN_FIRSTNAME}" \
      --lastname "${ADMIN_LASTNAME}" \
      --email "${ADMIN_EMAIL}" \
      --password "${ADMIN_PASSWORD}" 2>&1
  )
  status=$?
  set -e

  printf '%s\n' "${output}"

  if [ ${status} -eq 0 ]; then
    return 0
  fi

  if printf '%s' "${output}" | grep -qi "already exist"; then
    return 0
  fi

  return ${status}
}

configure_trino_database() {
  if superset set-database-uri --database_name "${TRINO_DATABASE_NAME}" --uri "${TRINO_SQLALCHEMY_URI}"; then
    return 0
  fi

  superset set-database-uri --database-name "${TRINO_DATABASE_NAME}" --uri "${TRINO_SQLALCHEMY_URI}"
}

mkdir -p /app/superset_home

echo_step "Applying Superset metadata migrations"
superset db upgrade

echo_step "Ensuring admin user ${ADMIN_USERNAME}"
create_admin_user

echo_step "Initializing roles and permissions"
superset init

echo_step "Configuring Trino connection ${TRINO_DATABASE_NAME}"
configure_trino_database

cat <<EOF

Superset is ready for the lab.
URL: http://localhost:8088
Login: ${ADMIN_USERNAME} / ${ADMIN_PASSWORD}
Database connection: ${TRINO_DATABASE_NAME}
SQLAlchemy URI: ${TRINO_SQLALCHEMY_URI}

EOF

exec /usr/bin/run-server.sh
