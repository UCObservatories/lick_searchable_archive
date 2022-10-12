import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone
from pathlib import Path


def setup_logging(log_path, log_name, log_level, log_tid=False):
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
    tid_format = " tid:{thread}" if log_tid else ""
    file_handler.setFormatter(logging.Formatter(fmt="{levelname:8} {asctime}" + tid_format + " {module}:{funcName}:{lineno} {message}", style='{'))

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
