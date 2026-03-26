#!/usr/bin/env python3
import os
from pathlib import Path

from superset.app import create_app


TRINO_DATABASE_NAME = os.environ.get("SUPERSET_TRINO_DATABASE_NAME", "trino_iceberg_lab")
TRINO_SQLALCHEMY_URI = os.environ.get(
    "SUPERSET_TRINO_SQLALCHEMY_URI",
    "trino://trino@trino:8080/iceberg",
)
ASSET_ROOT = Path(os.environ.get("SUPERSET_LAB_ASSET_ROOT", "/app/lab-assets"))
OUTPUT_ROOT = Path(os.environ.get("SUPERSET_LAB_RENDERED_BUNDLE_DIR", "/tmp/lab_bundle"))


def render_bundle(database_uuid: str) -> None:
    replacements = {
        "__TRINO_DATABASE_NAME__": TRINO_DATABASE_NAME,
        "__TRINO_SQLALCHEMY_URI__": TRINO_SQLALCHEMY_URI,
        "__TRINO_DATABASE_UUID__": database_uuid,
    }

    if OUTPUT_ROOT.exists():
        for path in sorted(OUTPUT_ROOT.rglob("*"), reverse=True):
            if path.is_file() or path.is_symlink():
                path.unlink()
            else:
                path.rmdir()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    for source in ASSET_ROOT.rglob("*"):
        relative = source.relative_to(ASSET_ROOT)
        target = OUTPUT_ROOT / relative
        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue

        content = source.read_text(encoding="utf-8")
        for old, new in replacements.items():
            content = content.replace(old, new)
        target.write_text(content, encoding="utf-8")


def normalize_database() -> str:
    app = create_app()
    with app.app_context():
        from superset.extensions import db
        from superset.models.core import Database

        databases = (
            db.session.query(Database)
            .filter(Database.database_name == TRINO_DATABASE_NAME)
            .order_by(Database.id.asc())
            .all()
        )
        if not databases:
            raise RuntimeError(f"Database {TRINO_DATABASE_NAME!r} was not found.")

        primary = databases[0]
        primary.set_sqlalchemy_uri(TRINO_SQLALCHEMY_URI)

        for duplicate in databases[1:]:
            duplicate.database_name = f"{TRINO_DATABASE_NAME}_legacy_{duplicate.id}"

        db.session.commit()
        return str(primary.uuid)


def main() -> None:
    database_uuid = normalize_database()
    render_bundle(database_uuid)
    print(database_uuid)


if __name__ == "__main__":
    main()
