""" Defines the schema used to store metadata for the Lick Archive.
Uses SQL Alchemy's ORM
"""
from enum import Enum as PythonEnum
from datetime import datetime, date

from sqlalchemy import Column, Float, String, Integer, Table, BigInteger, ForeignKey, Date, TIMESTAMP, Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Index
from sqlalchemy import Enum as SqlAlchemyEnum

from astropy.coordinates import SkyCoord


from lick_archive.db.pgsphere import SPoint
from lick_archive.db.bitstring import BitString
from lick_archive.data_dictionary import data_dictionary, IngestFlags, LargeInt, LargeStr


class Base(DeclarativeBase):
    pass


version = 1.0

# We build the Main table using SQL Alchemy's Core API so we can build it from the data dictionary

_primary_key = "id"
_unique = ['filename']
_required = ['filename', 'telescope', 'instrument', 'obs_date', 'frame_type', 'public_date']

# Define a default publication date far into the future, for help migrating existing data without a date
_defaults = {'public_date': date(9999,12,31).isoformat()}

def _map_type(python_type):
    type_map = {int :        Integer,
                str :        String,
                float :      Float,
                datetime:    TIMESTAMP,
                date:        Date,
                SkyCoord:    SPoint,
                IngestFlags: BitString,
                LargeInt:    BigInteger,
                LargeStr:    Text,
                }
    
    if python_type in type_map:
        return type_map[python_type]
    elif issubclass(python_type, PythonEnum):
        return SqlAlchemyEnum(python_type, values_callable=lambda x: [y.value for y in x])
    else:
        raise NotImplementedError(f"Python type {python_type} not supported when mapping to SQLAlchemy")


main_columns = [Column(dd_row['db_name'], _map_type(dd_row['type']), 
                       primary_key=True if dd_row['db_name'] in _primary_key else None,
                       unique=True if dd_row['db_name'] in _unique else None,
                       nullable=False if dd_row['db_name'] in _required else True,
                       server_default=_defaults.get(dd_row['db_name'],None),
                       ) 
                for dd_row in data_dictionary]
main = Table("main", Base.metadata,*main_columns)

class Main(Base):
    __table__ = main

Index('index_m_obs_date', Main.obs_date)
Index('index_m_instrument', Main.instrument)
Index('index_m_object', Main.object)
Index('index_m_frame', Main.frame_type)
Index('index_m_coord', Main.coord, postgresql_using='gist')


class VersionHistory(Base):
    __tablename__ = 'version_history'

    id = Column(Integer, primary_key=True)
    version = Column(String)
    event = Column(String)
    install_date = Column(TIMESTAMP(True))

Index('index_vh_install_date', VersionHistory.install_date)

class UserDataAccess(Base):
    __tablename__ = "user_data_access"

    file_id = Column(ForeignKey("main.id"), primary_key=True)
    obid = Column(Integer, primary_key=True)
    reason = Column(String)
"""
class CoverDataAccess(Base):
    __tablename__ = "cover_data_access"

    file_id = Column(ForeignKey("main.id"), primary_key=True)
    cover_id = Column(String, primary_key=True)
    reason = String()
"""

