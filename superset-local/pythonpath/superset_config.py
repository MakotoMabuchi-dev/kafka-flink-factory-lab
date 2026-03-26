import os


SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "lab-superset-secret-key")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_INTERNAL_SQLALCHEMY_URI",
    "sqlite:////app/superset_home/superset.db",
)

CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
RESULTS_BACKEND_USE_MSGPACK = False

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
