from datetime import datetime, timezone

from sqlalchemy import create_engine
from archive_schema import Base, VersionHistory, version

from db_utils import create_db_engine, open_db_session

engine = create_db_engine()

Base.metadata.create_all(engine)

session = open_db_session(engine)
session.add(VersionHistory(version=version, event="Create DB", install_date=datetime.now(timezone.utc)))
session.commit()