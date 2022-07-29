""" Defines the schema used to store metadata for the Lick Archive.
Uses SQL Alchemy's ORM
"""
import enum

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, Integer, Sequence, ForeignKey, TIMESTAMP, Text
from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy.orm import relationship
from sqlalchemy import Index, Enum
from pgsphere import SPoint

Base = declarative_base()

version = 0.1

class FrameType(enum.Enum):
    dark = enum.auto()
    flat = enum.auto()
    bias = enum.auto()
    science = enum.auto()
    arc = enum.auto()
    unknown = enum.auto()

class IngestFlags(enum.IntFlag):
    CLEAR               = 0        # Nothing of interest when ingesting the file
    NO_LAMPS_IN_HEADER  = 1        # No lamps were specified in the header, so OBJECT was used to find the type
    AO_NO_DATE_BEG      = 2        # A Shane AO/ShARCS file had no DATE_BEG
    AO_USE_DATE_OBS     = 4        # A Shane AO/ShARCS file had used DATE_OBS, which is less reliable
    USE_DIR_DATE        = 8        # The obs date for a file was determined by the directory name, so is only accurate to 24 hours.
    NO_OBJECT_IN_HEADER = 16       # There was no OBJECT in the header
    NO_FITS_END_CARD    = 32       # The FITS header had no END card
    NO_FITS_SIMPLE_CARD = 64       # The FITS header had no SIMPLE card at the beginning.
    FITS_VERIFY_ERROR   = 128      # The FITS header failed a verification check.
    UNKNOWN_FORMAT      = 256      # The FITS file could not be identified (used internally, should not be inserted to DB).
    NO_COORD            = 512      # The RA/DEC in the header could be parsed, so cone searches will not match it.
    INVALID_CHAR        = 1024     # An invalid character (such as '\x00') was found in the header.
    
class Main(Base):
    __tablename__ = 'main'

    id = Column(Integer, primary_key=True)

    # common fields
    telescope            = Column(Enum("Shane", name="telescope_enum"), nullable = False)
    instrument           = Column(Enum("Kast Blue", "Kast Red", "ShaneAO/ShARCS", name="instrument_enum"), nullable=False)
    obs_date             = Column(TIMESTAMP(True), nullable=False)
    exptime              = Column(Float)
    ra                   = Column(String)
    dec                  = Column(String)
    coord                = Column(SPoint)
    object               = Column(String)
    airmass              = Column(Float)
    frame_type           = Column(Enum(FrameType), nullable = False)
    filename             = Column(String, unique=True, nullable = False)
    program              = Column(String)
    observer             = Column(String)
    ingest_flags         = Column(BIT(32), nullable=False)
    header               = Column(Text)

    # shane kast fields
    slit_name            = Column(String)
    beam_splitter_pos    = Column(String)
    grism                = Column(String)
    grating_name         = Column(String)
    grating_tilt         = Column(Integer)

    # shane ao/sharcs fields
    apername             = Column(String)
    filter1              = Column(String)
    filter2              = Column(String)
    sci_filter           = Column(String)
    coadds_done          = Column(Integer)
    true_int_time        = Column(Float)


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