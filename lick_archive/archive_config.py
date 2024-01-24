from dataclasses import dataclass
from pathlib import Path
import sys
import enum

from lick_archive.script_utils import ConfigBase, ConfigFile, ParsedURL, PostgreSQLURL

class ArchiveSiteType(enum.Enum):
    SINGLE_HOST = "single_host"
    FRONTEND = "frontend"
    BACKEND = "backend"


@dataclass
class HostConfig(ConfigBase):
    """Configuration values that may vary by host"""
    config_section_name = "host"
    
    type            : ArchiveSiteType
    """The type of host"""

    app_names       : list[str]
    """The apps deployed to the host"""

    url_path_prefix : str
    """Path prefix to use for archive URLs (e.g. example.org/prefix/<all archive URLs>)"""

    api_url         : ParsedURL
    """The URL used to access the backend API"""

    frontend_url    : ParsedURL
    """The URL used to access the frontend"""

@dataclass
class DatabaseConfig(ConfigBase):
    """Database configuratoin values"""
    config_section_name = "database"

    archive_db     : str
    """The name of the archive metadata database"""

    db_query_user  : str
    """The name of the user queries should use. This user has read-only access."""

    db_ingest_user : str
    """The name of the user ingest should use. This user has read/write access."""

@dataclass
class QueryConfig(ConfigBase):
    """Query configuration"""
    config_section_name = "query"

    file_header_url_format : str
    """The python string format for forming the URL to a file's header. The {} is replaced by the files relative path in the archive."""

    default_search_radius  : str
    """Default search radius when searching by ra and dec. This can be in any format astropy.coordinates.Angle can recognize."""


@dataclass
class IngestConfig(ConfigBase):
    """Metadata ingest configuration."""
    config_section_name = "ingest"

    archive_root_dir           : Path
    """The root directory of the archive file system."""

    default_proprietary_period : str
    """The default proprietary period for files."""

    sched_db_host : str
    """The schedule database host (with optional port specified after a colon)"""
    
    sched_db_name : str
    """The schedule database name"""

    sched_db_user_info : str
    """Path to a text file containing the schedule database's user information, formatted as 'user:password'"""

    supported_instruments : list[str]
    """The instrument directory names supported by the archive."""

class ArchiveConfigFile(ConfigFile):
    config_classes = [HostConfig, DatabaseConfig, QueryConfig, IngestConfig]
    @classmethod
    def load_from_standard_inifile(cls):
        # This relies on our current way of deploying the config into "etc" under a python virtual environment
        settings_file = Path(sys.executable).parent.parent / "etc" / "archive_config.ini"

        return cls.from_file(settings_file)
    

