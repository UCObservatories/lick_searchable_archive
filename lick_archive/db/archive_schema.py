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

"""
Old main
class Main(Base):
    __tablename__ = 'main'

    # common fields
    id                   = Column(Integer, primary_key=True, 
                                  info=FieldInfo("Internal Id", DEFAULT_GROUP, 'Unique Internal integer ID of the file.'))
    telescope            = Column(Enum(Telescope, values_callable=lambda x: [y.value for y in x], name="telescope_enum"), nullable = False,
                                  info=FieldInfo("Telescope", DEFAULT_GROUP, 'Name of telescope, e.g. “Shane”'))
    instrument           = Column(Enum(Instrument, values_callable=lambda x: [y.value for y in x], name="instrument_enum"), nullable=False,
                                  info=FieldInfo("Instrument", DEFAULT_GROUP, 'Name of instrument, e.g. "Kast Blue" or "ShaneAO/ShARCS"'))
    obs_date             = Column(TIMESTAMP(True), nullable=False,
                                  info=FieldInfo("Observation Date", DEFAULT_GROUP, "UTC Observation date and time."))
    exptime              = Column(Float,
                                  info=FieldInfo("Exposure Time", DEFAULT_GROUP, "Exposure time in seconds."))
    ra                   = Column(String,
                                  info=FieldInfo("Right Ascension", DEFAULT_GROUP, "Right Ascension in decimal degrees."))
    dec                  = Column(String,
                                  info=FieldInfo("Declination", DEFAULT_GROUP, "Declination in decimal degrees."))
    coord                = Column(SPoint)
    object               = Column(String,
                                  info=FieldInfo("Object", DEFAULT_GROUP, "Name/description of object being observed."))
    airmass              = Column(Float,
                                  info=FieldInfo("Airmass", DEFAULT_GROUP, "Airmass of the observation."))
    frame_type           = Column(Enum(FrameType, values_callable=lambda x: [y.value for y in x], name="frame_type_enum"), nullable = False,
                                  info=FieldInfo("Frame Type", DEFAULT_GROUP, f" ""Type of the observation. One of {','.join(['"' + frame_type.value + '"' for frame_type in FrameType])}."" "))
    filename             = Column(String, unique=True, nullable = False,
                                  info=FieldInfo("File Location", DEFAULT_GROUP, "Relative filename and path within the archive filesystem."))
    program              = Column(String,
                                  info=FieldInfo("Program", DEFAULT_GROUP, 'The name of the program the observation was taken for, e.g. "2019A_S013".'))
    observer             = Column(String,
                                  info=FieldInfo("Observer", DEFAULT_GROUP, "The name of the person taking the observation."))
    ingest_flags         = Column(BitString, nullable=False)
    header               = Column(Text, info=FieldInfo("Header", DEFAULT_GROUP, "The full header information from the file in plain text format."))

    # shane kast fields
    slit_name            = Column(String, info=FieldInfo("Slit Name", SHANE_KAST_GROUP, "The slit name."))
    beam_splitter_pos    = Column(String, info=FieldInfo("Beam Splitter Position", SHANE_KAST_GROUP, "The beam splitter position"))
    grism                = Column(String, info=FieldInfo("Grism", SHANE_KAST_GROUP, "The grism used."))
    grating_name         = Column(String, info=FieldInfo("Grating Name", SHANE_KAST_GROUP, "The grating used."))
    grating_tilt         = Column(Integer, info=FieldInfo("Grating Tilt", SHANE_KAST_GROUP, "The grating tilt used."))

    # shane ao/sharcs fields
    apername             = Column(String, info=FieldInfo("Aperture Position", SHARCS_GROUP, "Dewar aperture wheel, named position"))
    filter1              = Column(String, info=FieldInfo("Filter 1", SHARCS_GROUP, "Dewar filter wheel 1, named position"))
    filter2              = Column(String, info=FieldInfo("Filter 2", SHARCS_GROUP, "Dewar filter wheel 2, named position"))
    sci_filter           = Column(String, info=FieldInfo("Science Filter", SHARCS_GROUP, "External (warm) science filter wheel position"))
    coadds_done          = Column(Integer, info=FieldInfo("Number of Coadds", SHARCS_GROUP, "Number of coadds."))
    true_int_time        = Column(Float, info=FieldInfo("True Integration Time", SHARCS_GROUP, "True integration time in seconds per coadd"))

    # Authorization fields
    #public               = Column(Boolean, info=FieldInfo("Is Public", None, "True if the file is public."))
"""
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

