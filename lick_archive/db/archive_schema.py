""" Defines the schema used to store metadata for the Lick Archive.
Uses SQL Alchemy's ORM
"""
import enum
from collections import namedtuple
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, Integer, Sequence, ForeignKey, TIMESTAMP, Text
from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy.orm import relationship
from sqlalchemy import Index, Enum
from lick_archive.db.pgsphere import SPoint
from lick_archive.db.bitstring import BitString

Base = declarative_base()

version = 0.1

class FrameType(enum.Enum):
    dark    = "dark"
    flat    = "flat"
    bias    = "bias"
    science = "science"
    arc     = "arc"
    unknown = "unknown"

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

class Telescope(enum.Enum):
    SHANE = "Shane"

class Instrument(enum.Enum):
    KAST_RED =  "Kast Red"
    KAST_BLUE = "Kast Blue"
    SHARCS =    "ShaneAO/ShARCS"


# Field Info for meta information about fields
# TODO: Make a full DataDictionary class with this info?
FieldInfo = namedtuple("FieldInfo", ["user_name", "user_group", "descr"])


# Constants to prevent typos in group names
DEFAULT_GROUP = "Common Fields"
SHANE_KAST_GROUP = "Shane Kast Specific"
SHARCS_GROUP = "Shane AO/ShARCS Specific"

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
                                  info=FieldInfo("Frame Type", DEFAULT_GROUP, f"""Type of the observation. One of {','.join(['"' + frame_type.value + '"' for frame_type in FrameType])}."""))
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

all_attributes = [col.name for col in Main.__table__.columns]
indexed_attributes = ['filename', 'date', 'datetime', 'object', 'ra_dec']
allowed_sort_attributes = [col.name for col in Main.__table__.columns if col.name not in ['coord', 'header', 'ingest_flags']]
allowed_result_attributes =[col.name for col in Main.__table__.columns if col.name not in ['coord','ingest_flags']]

field_info = {col.name: col.info for col in Main.__table__.columns if col.name not in ['coord','ingest_flags']}

