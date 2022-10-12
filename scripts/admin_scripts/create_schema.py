#!/usr/bin/python3
""" Create the Lick Archive schema in an existing empty database. """
from datetime import datetime, timezone

from lick_archive.db.archive_schema import Base, VersionHistory, version

from lick_archive.db.db_utils import create_db_engine, open_db_session

engine = create_db_engine()

Base.metadata.create_all(engine)

session = open_db_session(engine)
session.add(VersionHistory(version=version, event="Create DB", install_date=datetime.now(timezone.utc)))
session.commit()

print("Schema created successfully.")