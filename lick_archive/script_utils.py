from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone, date
import time
from pathlib import Path
import re
import configparser
import abc
import typing
from urllib.parse import urlparse
import types
import inspect

def get_std_log_formatter(log_tid=False, log_pid=False):
    """Return a log formatter with the "standard" for the lick searchable archive.
    This format includes the log level, time, the thread id, line of code + the message.
    The time is always UTC and is to the millisecond.

    In Python 3.10+ this method may no longer be needed as the special '.' entry in configuration
    dicts should be able to set the additional attributes.

    Args:
    log_pid (bool):  Whether or not to log process ids. Defaults to False.
    log_tid (bool):  Whether or not to log thread ids. Defaults to False.

    Return (logging.Formatter): A formatter for the Python built in logging facility.
    """
    optional_format = ""
    if log_pid:
        optional_format += "pid:{process} "
    if log_tid:
        optional_format += "tid:{thread} "

    formatter = logging.Formatter(fmt="{levelname:8} {asctime} " + optional_format + "{module}:{funcName}:{lineno} {message}",                                   
                                  style='{')
    # If these two attributes
    formatter.converter=time.gmtime
    formatter.default_msec_format = "%s.%03d"
    return formatter

def setup_logging(log_path, log_name, log_level, log_tid=False, log_pid=False):
    """Setup loggers to send some information to stderr and some to a file. The logging level for stderr is
    always set to INFO, but for the log file it is configurable.
    
    Args:
    log_path (str or None): Directory to place a log file. The log will be in the current directory if this is None.

    log_name (str):  Name of the log file. The full name will be <log_path>/<log_name>_<timestamp>.log
    
    log_level (str): The logging level of messages to send to the log file. One of CRITICAL, ERROR, WARNING, INFO
                     DEBUG, NOTSET.

    log_tid (bool):  Whether or not to log thread ids. Defaults to False.
               
    """

    log_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    log_file = f"{log_name}_{log_timestamp}.log"
    if log_path is not None:
        log_file = Path(log_path).joinpath(log_file)

    # Configure a file handler to write detailed information to the log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(get_std_log_formatter(log_tid))

    # Setup a basic formatter for output to stderr
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter())

    logging.basicConfig(handlers=[stream_handler, file_handler], level=logging.DEBUG)

def setup_service_logging(log_path, log_name, log_level, rollover_days, backup_count, log_tid=False):
    """Setup loggers for services. Services log everything to a file, and will roll-over and delete old logs.
    
    Args:
    log_path (str or None): Directory to place a log file. The log will be in the current directory if this is None.

    log_name (str):  Name of the log file. The full name will be <log_path>/<log_name>_<timestamp>.log
    
    log_level (str): The logging level of messages to send to the log file. One of CRITICAL, ERROR, WARNING, INFO
                     DEBUG, NOTSET.

    rollover_days (int): How many days between log rollovers.

    backup_count (int): How many old logs to keep.
               
    log_tid (bool):  Whether or not to log thread ids. Defaults to False.
    """

    log_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    log_file = f"{log_name}_{log_timestamp}.log"
    if log_path is not None:
        log_file = Path(log_path).joinpath(log_file)

    # Configure a timed rotating file handler to write to the log file
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when = 'D', interval=rollover_days, backupCount=backup_count)
    file_handler.setLevel(log_level)
    tid_format = " tid:{thread}" if log_tid else ""
    file_handler.setFormatter(logging.Formatter(fmt="{levelname:8} {asctime}" + tid_format + " {module}:{funcName}:{lineno} {message}", style='{'))

    logging.basicConfig(handlers=[file_handler], level=logging.DEBUG)

def get_unique_file(path, prefix, extension=""):
    """
    Return a unique filename.

    Args:
    path (pathlib.Path): Path where the unique file will be located.
    prefix (str):  Prefix name for the file.
    extension (str): File extension for the file. Defaults to empty.

    Returns:
    A filename starting with prefix, ending with extension, that does not currently
    exist.
    """
    if extension != "" and not extension.startswith('.'):
        extension = '.' + extension            

    unique_file = path.joinpath(prefix + extension)
    n = 1
    while unique_file.exists():
        unique_file = path.joinpath(prefix + f".{n}" + extension)
        n+=1
    return unique_file

def parse_date_range(date_range):
    """
    Parse a date range from the command line. The date range's format is "YYYY-MM-DD" indicating a single day or 
    "YYYY-MM-DD:YYYY-MM-DD" indicating a range.

    Returns:
    start_date: A datetime.date of the start of the date range.
    end_date: A datetime.date of the end of the date range.
    """
    if date_range is not None:
        if ":" in date_range:
            (start_str, end_str) = date_range.split(":")
        else:
            start_str = date_range
            end_str = None

        date_list = start_str.split('-')
        if len(date_list) < 3:
            raise ValueError(f"'{start_str}' is an invalid start date. It should be YYYY-MM-DD.")

        try:
            start_date = date(int(date_list[0]), int(date_list[1]), int(date_list[2]))
        except:
            raise ValueError(f"'{start_str}' is an invalid start date. It should be YYYY-MM-DD.")

        if end_str is None:
            end_date = start_date
        else:
            date_list = end_str.split('-')
            if len(date_list) < 3:
                raise ValueError(f"'{end_str}' is an invalid end date. It should be YYYY-MM-DD.")

            try:
                end_date = date(int(date_list[0]), int(date_list[1]), int(date_list[2]))
            except:
                raise ValueError(f"'{end_str}' is an invalid start date. It should be YYYY-MM-DD.")

        return (start_date, end_date)
    return (None, None)

def get_files_for_daterange(root_dir, start_date, end_date, instruments):
    """
    Scan the lick archive root dir for files that match command line parameters.

    The direcotires in the archive are expected to follow the 'YYYY-MM/DD/instrument/' convention.

    Args:

    root_dir:    (str) The root directory of the archive
    start_date:  (datetime.date) The starting date of the date range returned files should lie within.
                                 None for no date range. 
    end_date:    (datetime.date) The ending date of the date range returned files should lie within
                                 None for no date range.
    instruments: (list of str) A list of instruments to find files for.

    Returns: A generator for the list of matching pathlib.Path objects.
    """
    root_path = Path(root_dir)

    # Go through the month directories
    for month_dir in root_path.iterdir():
        if month_dir.is_dir():
            # This should be a directory of the format YYYY-MM
            match = re.match(r'^(\d\d\d\d)-(\d\d)$', month_dir.name)
            if match is not None:
                year = int(match.group(1))
                month = int(match.group(2))
                # Go through the day directories
                for day_dir in month_dir.iterdir():
                    if day_dir.is_dir() and re.match(r'^\d\d$',day_dir.name) is not None:
                        day = int(day_dir.name)
                        # Build a date object and if it's between the requested date range (inclusive), keep searching this
                        # directory
                        current_date = date(year, month, day)
                        if start_date is None or (current_date >= start_date and current_date <= end_date):
                            # Go through instrument directories
                            for instrument_dir in day_dir.iterdir():                                
                                # Return any files found for the requested instruments
                                if instrument_dir.is_dir() and instrument_dir.name in instruments:
                                    for file in instrument_dir.iterdir():
                                        if not file.is_file():
                                            print(f"Unexpected directory or special file: {file}")
                                            continue
                                        yield file

class ParsedURL:
    """A class representing a parsed URL that can still be passed as an URL to requests.
    Currently only http and https URLs are allowed.

    Args:
       url: The URL to parse

    Attributes:
        url    (str): The whole URL
    Raises:
        ValueError if the value is not a valid URL
    
    """
    def __init__(self, url : str):
        result = urlparse(url)
        if result.scheme not in  ['http', 'https'] or result.netloc == '':
            raise ValueError(f"{url} is not a valid http or https URL.")
        
        self.url = url

    def __str__(self) -> str:
        """Return URL as a string"""
        return self.url
    
    def __add__(self, other : str) -> ParsedURL:
        """Return a new URL with a string appended to it."""
        return ParsedURL(self.url + other)


class ConfigBase(abc.ABC):

    def validate(self):
        """Method to perform post parsing validation. This is intended to be inherited by 
        subclasses and is called after creating the config class from an ini file.
        
        Child classes should raise an exception if the configuration is not valid. Otherwise
        They should return the validated object.  The default implementation simply returns self.
        """
        return self
        
    @classmethod
    def from_config_section(cls, config_section):

        # Find the init arguments the subclass takes
        init_signature = inspect.signature(cls.__init__)

        # Build the keyword arguments to build the subclass
        child_init_kwargs = {}
        all_validation_errors = []
        for attr in init_signature.parameters:

            if attr == "self":
                # Don't count "self"
                continue

            # Keep track of errors validating a value
            attr_validation_errors = []
        
            # For optional or union arguments, build a list of possible types
            attr_type =  init_signature.parameters[attr].annotation
            type_origin = typing.get_origin(attr_type)
            if type_origin is None:
                # Not a special annotated type
                possible_types = typing.get_args(attr_type)
                if len(possible_types) == 0:
                    possible_types = [attr_type]
            elif (type_origin is typing.Union) or (type_origin is typing.Optional) or (type_origin is types.UnionType):
                # Types that accept multiple types
                possible_types = typing.get_args(attr_type)
            else:
                # Let _read_type_from_config deal with it
                possible_types = [attr_type]

            if len(possible_types) == 0:
                raise ValueError(f"Config class '{cls.__name__}' __init__ method uses unsupported type '{attr_type}' for parameter '{attr}'")
    
            # Populate default
            if init_signature.parameters[attr].default != inspect.Parameter.empty:
                child_init_kwargs[attr] = init_signature.parameters[attr].default

            # Try each possible type to see if it can parse the configuration
            for t in possible_types:
                try:
                    child_init_kwargs[attr] = cls._read_type_from_config(t, config_section, attr)
                    break
                except Exception as e:
                    # Keep track of validation errors
                    attr_validation_errors.append(str(e))
    
            msg =  f"Config section '[{config_section.name}]' attribute '{attr}'"
            if len(attr_validation_errors) > 0:
                msg += "\n    " + "\n    ".join(attr_validation_errors)

            if attr not in child_init_kwargs:               
                # Make sure at least one of the possible types matched
                raise ValueError(msg)
            else:
                if len(attr_validation_errors) > 0:
                    # Keep track of the errors in case there's an issue building the object
                    all_validation_errors.append(msg)

        # Try to build the object with the parsed data    
        try:
            result = cls(**child_init_kwargs).validate()
            return result
        except Exception as e:
            raise ValueError(f"Failed to build {cls.__name__} from '[{config_section.name}]':\n{e}\n" + "\n".join(all_validation_errors))
    
    @classmethod
    def _read_type_from_config(cls, type_object : typing.Callable, config_section : typing.Mapping, attribute_name : str) -> typing.Any:

        type_origin = typing.get_origin(type_object)
        
        if type_origin is not None:
            type_args = typing.get_args(type_object)
            type_object = type_origin
        else:
            type_args = []

        if not isinstance(type_object, type):
            # This could happen if the type is something like "typing.Annotated"
            raise ValueError(f"Unsupported type '{type_object}'")

        # Look for a nested subclass. Note this may not have an option in the ini file
        elif issubclass(type_object, ConfigBase):
            # Let a nested subclass parse itself
            return type_object.from_config_section(config_section)
       
        elif attribute_name not in config_section:
            raise ValueError(f"Missing attribute '{attribute_name}'")
        elif type_object is list or type_object is tuple or type_object is set:
            # Sequence types
            string_values = config_section[attribute_name].split(",")
            # See if there's typing
            if len(type_args) == 0:
                typed_values = string_values
            elif len(type_args) == 1:
                # One type for every element
                typed_values= [cls._parse_value(type_object=type_args[0],value=v) for v in string_values]
            elif len(type_args) == len(string_values):
                # Each element must match it's corresponding type
                typed_values= [cls._parse_value(type_object=t,value=v) for v, t in zip(string_values, type_args)]
            else:
                raise ValueError(f"Length of {type_object.__name__} {len(string_values)} does not match expected length {len(type_args)}")
            return type_object(typed_values)        
        else:
            return cls._parse_value(type_object=type_object, value=config_section[attribute_name])
    
    @classmethod
    def _parse_value(cls, type_object : typing.Callable, value: str) -> typing.Any:
        if type_object is types.NoneType:
            # Return optional values as None if they're empty
            if value is None or len(value) == 0:
                return None
        
        elif type_object is str:
            # Make sure strings aren't empty
            if len(value) == 0:
                raise ValueError(f"Empty value for")
            else:
                return value
        elif type_object is bool:
            # Parse the boolean allowing true/false 1/0 or yes/no.
            if value.lower() in ["true", "1", "yes"]:
                return True
            elif value.lower() in ["false", "0", "no"]:
                return False
            else:
                raise ValueError(f"Invalid boolean value '{value}'")
        else:
            # For other types, let the type_object constructor do the parsing
            return type_object(value)

class ConfigFile:
    config_classes = []
    config = None

    def __init__(self, sections):
        for section, object in sections.items():
            self.__setattr__(section, object)

    @classmethod
    def from_file(cls, file : str | Path) -> ConfigFile:

        if cls.config is None:
            config_parser = configparser.ConfigParser()
            config_parser.read(file)
            sections = {}
            for config_cls in cls.config_classes:
                if hasattr(config_cls, "config_section_name"):
                    section_name = config_cls.config_section_name
                else:
                    section_name = config_cls.__name__.removesuffix("Config").lower()
                
                if section_name not in config_parser.sections():
                    raise ValueError(f"Config file {file} missing '{section_name}' section.")
                
                config_section = config_parser[section_name]

                sections[section_name] = config_cls.from_config_section(config_section)

            cls.config = cls(sections)
        return cls.config
