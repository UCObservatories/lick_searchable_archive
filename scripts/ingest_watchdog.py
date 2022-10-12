"""
Service that watches for new ingests and notifies the Lick archive about them.
"""
import argparse
import configparser
from pathlib import Path
import sys
import os
import logging
from functools import partial
import datetime
import threading
from collections import OrderedDict, namedtuple
from time import sleep
from urllib.parse import urlparse
from requests.exceptions import RequestException

import watchdog
import watchdog.events
import watchdog.observers.api
import watchdog.observers.polling

from lick_archive.script_utils import setup_service_logging
from lick_archive.lick_archive_client import LickArchiveIngestClient

logger = logging.getLogger(__name__)

_CONFIG_KEY = 'ingest_watchdog'
_CONFIG_DATA_ROOT = 'data_root'
_CONFIG_METHOD = 'method'
_CONFIG_POLLING_INTERVAL = 'polling_interval'
_CONFIG_POLLING_SEARCHES = 'polling_searches'
_CONFIG_WRITE_DELAY = 'polling_write_delay'
_CONFIG_INOTIFY_AGE = 'inotify_age'
_CONFIG_INGEST_URL = 'ingest_url'
_CONFIG_INST_DIRS = 'instrument_dirs'
_CONFIG_RETRY_MAX_DELAY = 'ingest_retry_max_delay'
_CONFIG_RETRY_MAX_TIME = 'ingest_retry_max_time'
_CONFIG_REQUEST_TIMEOUT = 'ingest_request_timeout'
_CONFIG_STARTUP_RESYNC_AGE = 'startup_resync_age'

def validate_int(parsed_config, section, key):
    try:
        value = parsed_config[section].getint(key)
        if value is None:
            raise ValueError(f"Config missing '{key}' setting under '[{section}]'.")
        return value
    except ValueError as e:
        raise ValueError(f"Config setting '{key}'  under '[{section}]': {e}.")       

def validate_url(parsed_config, section, key):
    try:
        value = parsed_config[section].get(key)
        if value is None:
            raise ValueError(f"Config missing '{key}' setting under '[{section}]'.")

        result = urlparse(value)
        if result.scheme not in  ['http', 'https'] or result.netloc == '' or result.path == '':
            raise ValueError(f"Config setting '{key}' under '[{section}] is not a valid http or https URL.")

        return value
    except Exception as e:
        raise ValueError(f"Config setting '{key}'  under '[{section}]': {e}.")       


def validate_float(parsed_config, section, key):
    try:
        value = parsed_config[section].getfloat(key)
        if value is None:
            raise ValueError(f"Config missing '{key}' setting under '[{section}]'.")
        return value
    except ValueError as e:
        raise ValueError(f"Config setting '{key}'  under '[{section}]': {e}.")       


def validate_list(parsed_config, section, key):
    value = parsed_config[section].get(key)
    if value is None:
        raise ValueError(f"Config missing '{key}' setting under '[{section}]'.")

    l = [item.strip() for item in value.split(",")]
    for item in l:
        if len(item) == 0:
            raise ValueError(f"Config setting '{key}'  under '[{section}]' has empty values.")

    return l

def validate_not_empty(parsed_config, section, key):
    value = parsed_config[section].get(key)
    if value is None:
        raise ValueError(f"Config missing '{key}' setting under '[{section}]'.")
    if len(value) == 0:
        raise ValueError(f"Config setting '{key}'  under '[{section}]' is empty.")
    return value


def validate_path(parsed_config, section, key, exists=False, is_dir=False):
    value = parsed_config[section].get(key)
    if value is None:
        raise ValueError(f"Config missing '{key}' setting under '[{section}]'.")
    if len(value) == 0:
        raise ValueError(f"Config setting '{key}'  under '[{section}]' is empty.")
    
    path = Path(value)

    if exists:
        if not path.exists():            
            raise ValueError(f"Config setting '{key}'  under '[{section}]' must be a file that exists")

    if is_dir:
        if not path.is_dir():
            raise ValueError(f"Config setting '{key}'  under '[{section}]' must be a directory that exists")

    return path


def parse_and_validate_polling_config(parsed_config, section):
    polling_fields = [ "interval", "searches", "write_delay" ]

    PollingConfig = namedtuple("PollingConfig", polling_fields)

    polling_interval = validate_int(parsed_config, section, _CONFIG_POLLING_INTERVAL)

    unparsed_searches = validate_list(parsed_config, section, _CONFIG_POLLING_SEARCHES)

    polling_searches = []
    Search = namedtuple("Search", ["interval", "age"])

    for search in unparsed_searches:
        search_contents = search.split(":")            
        if len(search_contents) != 2:
            raise ValueError(f"Invalid search value {search} for '{_CONFIG_POLLING_SEARCHES}' setting under '[{_CONFIG_KEY}]'.")

        try:
            polling_searches.append(Search(int(search_contents[0]), int(search_contents[1])))
        except ValueError:
            raise ValueError(f"Invalid search value '{search}' for '{_CONFIG_POLLING_SEARCHES}' setting under '[{_CONFIG_KEY}]'.")

    polling_write_delay = validate_int(parsed_config, section, _CONFIG_WRITE_DELAY)

    return PollingConfig(polling_interval, polling_searches, polling_write_delay)

def parse_and_validate_ingest_config(parsed_config, section):
    ingest_fields = [ "url", "retry_max_delay", "retry_max_time", "request_timeout" ]

    IngestConfig = namedtuple("IngestConfig", ingest_fields)

    url = validate_url(parsed_config, _CONFIG_KEY, _CONFIG_INGEST_URL)
    retry_max_delay = validate_int(parsed_config, _CONFIG_KEY, _CONFIG_RETRY_MAX_DELAY)
    retry_max_time = validate_int(parsed_config, _CONFIG_KEY, _CONFIG_RETRY_MAX_TIME)
    request_timeout = validate_float(parsed_config, _CONFIG_KEY, _CONFIG_REQUEST_TIMEOUT)

    return IngestConfig(url, retry_max_delay, retry_max_time, request_timeout)

def parse_and_validate_config(parsed_config):

    config_fields = ("data_root", "startup_resync_age", "method", "polling", "inotify_age", "instrument_dirs", "ingest")

    if _CONFIG_KEY not in parsed_config:
        raise ValueError(f"Config missing {_CONFIG_KEY} section.")

    data_root = validate_path(parsed_config, _CONFIG_KEY, _CONFIG_DATA_ROOT, is_dir=True)

    method = validate_not_empty(parsed_config, _CONFIG_KEY, _CONFIG_METHOD)
    if method == 'polling':
        polling_config = parse_and_validate_polling_config(parsed_config, _CONFIG_KEY)
        inotify_age = None
    elif method == 'inotify':
        polling_config = None
        inotify_age = validate_int(parsed_config, _CONFIG_KEY, _CONFIG_INOTIFY_AGE)
    else:
        raise ValueError(f"Unknown watchdog method '{method}' for '{_CONFIG_METHOD}' setting under '[{_CONFIG_KEY}]'.")

    instrument_dirs = validate_list(parsed_config, _CONFIG_KEY, _CONFIG_INST_DIRS)

    ingest_config = parse_and_validate_ingest_config(parsed_config, _CONFIG_KEY)

    IngestWatchdogConfig = namedtuple("IngestWatchdogConfig", config_fields)

    startup_age = validate_int(parsed_config, _CONFIG_KEY, _CONFIG_STARTUP_RESYNC_AGE)

    return IngestWatchdogConfig(data_root, startup_age, method, polling_config, inotify_age, instrument_dirs, 
                                ingest_config)

def logging_scandir(path):
    logger.debug(f"Scanning {path}")
    return os.scandir(path)


def logging_stat(path):
    logger.debug(f"Stating {path}")
    result = os.stat(path)
    return result


class PollingWithSimulatedCloseEmitter(watchdog.observers.polling.PollingEmitter):
    """ 
    Extends the watchdog package's PollingEmitter class with the ability to send a "Close" event
    a configurable delay after a file has been created (or modified). This allows time for a file to be fully 
    written to disk, and allows clients to treat the events from polling like they would from an
    inotify observer.

    Args:
    event_queue:    Event queue that receives events.
    watch:          Watch object representing the path being watched.
    timeout:        Polling interval between scans of the watched directory.
    writing_delay:  How long to wait (in seconds) after a file is created to send a close event.
    stat:           Function used to stat files.
    listdir:        Function used to scan directories.
    """
    def __init__(self, event_queue, watch, timeout, writing_delay, stat=os.stat, listdir=os.scandir):
        super().__init__(event_queue, watch, timeout, stat=logging_stat, listdir=logging_scandir)
        self._writing_delay = datetime.timedelta(seconds=writing_delay)
        self._file_modify_map = OrderedDict()
        self._file_modify_lock = threading.Lock()
    
    def queue_events(self, timeout):
        """
        Scan the watched directory and issue events.
        
        This overridden version calls the superclass method to do the scan, and then
        sends a close event for each file in its own internal map older than the writing delay.  

        Args:
        timeout (int): From the watchdog framework, this is the time the superclass method sleeps before
                       scanning the directory. 
        """
        super().queue_events(timeout)

        try:
            # Make sure nothing has stopped this thread while the scan was happening
            with self._lock:
                if not self.should_keep_running():
                    return

            # Go through the _file_modify_map and find any files older than the writing delay
            # Multiple threads can call queue_event, which also uses the file_modify_map.
            # So we protect the map with _file_modify_lock
            current_time = datetime.datetime.now(tz=datetime.timezone.utc)
            files_to_queue = []
            with self._file_modify_lock:
                
                # Get a list of files in the map. We don't directly iterate over keys
                # because we'll be removing files from the map as we go which would invalidate
                # iteration
                file_list = list(self._file_modify_map.keys())
                for file in file_list:
                    if  current_time - self._file_modify_map[file]  > self._writing_delay:
                        files_to_queue.append(file)
                        del self._file_modify_map[file]
        
            # Now queue the close events for files older than the writing delay
            for file in files_to_queue:
                self.queue_event(watchdog.events.FileClosedEvent(file))
        except Exception as e:
            # Letting exceptions escape this method will kill this thread, preventing future
            # scans of the directory
            logger.error(f"Exception in queue_events {e}", exc_info=True)

    def queue_event(self, event):
        """
        Queue a file system event for this emitter. This method keeps track of new files
        (or new modifications to files) that will need a close event sent for them before
        calling the superclass method to queue the event.

        Args:
        event: The file system event to queue.
        """
        current_time = datetime.datetime.now(tz=datetime.timezone.utc)

        if not event.is_directory:
            # Update the _file_modify_map with new and modified files, and delete any files
            # that were deleted before _writing_delay seconds passed.
            with self._file_modify_lock:
                if event.event_type in (watchdog.events.EVENT_TYPE_CREATED, watchdog.events.EVENT_TYPE_MODIFIED):
                    self._file_modify_map[event.src_path] = current_time
                elif event.src_path in self._file_modify_map:
                    if event.event_type in (watchdog.events.EVENT_TYPE_DELETED, watchdog.events.EVENT_TYPE_MOVED):
                        # File was deleted
                        del self._file_modify_map[event.src_path]

        super().queue_event(event)

class PollingWithSimulatedCloseObserver(watchdog.observers.api.BaseObserver):
    def __init__(self, timeout, writing_delay, stat, listdir):
        emitter_cls = partial(PollingWithSimulatedCloseEmitter, writing_delay=writing_delay, timeout=timeout, stat=stat, listdir=listdir)
        super().__init__(emitter_cls, timeout)

    def start(self):
        logger.debug("Starting")
        super().start()




class IngestWatcher(watchdog.events.FileSystemEventHandler):

    class PathInfo:
        def __init__(self, path, is_ingest_dir,observer=None, watch=None):
            self.path = path
            self.observer = observer
            self.watch = watch
            self.is_ingest_dir = is_ingest_dir

        def __eq__(self, other):
            return self.path == other.path

        def __hash__(self):
            return self.path.__hash__()

    def __init__(self, config, logger, client):
        self.logger = logger
        self.config = config
        self.archive_client = client

        self._path_info_map = dict()
        self._observer_list = []
        self._lock = threading.Lock()

    def start_observers(self, current_date):
        self.restart_observers(current_date, startup=True)

    def restart_observers(self, current_date, startup=False):
        with self._lock:
            self._path_info_map.clear()
            self._observer_list.clear()

            if self.config.method == "polling":
                self._reset_polling_observers(current_date)
            else:
                self._reset_inotify_observers(current_date)

        # Start the observer event threads
        self.start()

        # If we're not on initial startup, do a one day resync as a sanity check
        if not startup:
            self.resync(self, current_date, 1)

    def _reset_polling_observers(self, current_date):
        
        # We create one observer per "search". We also want to avoid having more than one observer watching the same
        # path. So we sort the searches by interval so that any paths covered by two searches are watched
        # by the observer wtih the shortest interval
        for search in sorted(self.config.polling.searches, key = lambda x: x.interval):        

            observer = PollingWithSimulatedCloseObserver(search.interval, self.config.polling.write_delay, os.stat, logging_scandir)
            self._observer_list.append(observer)
            path_info_list = self._get_paths_for_age(current_date, search.age)
            for path_info in path_info_list:
                if path_info.path in self._path_info_map:
                    # Don't watch a path with more than one observer
                    continue
                path_info.observer = observer
                self._path_info_map[path_info.path] = path_info
                self._watch(path_info)

    def _reset_inotify_observers(self, current_date):

        observer = watchdog.observers.inotify.InotifyObserver(generate_full_events=True)
        self._observer_list.append(observer)

        path_info_list = self._get_paths_for_age(current_date, self.config.inotify_age)
        for path_info in path_info_list:
            path_info.observer = observer
            self._path_info_map[path_info.path] = path_info
            self._watch(path_info)

    def _get_paths_for_age(self, current_date, age_in_days):
        paths = set()
        paths.add(IngestWatcher.PathInfo(self.config.data_root, False))
    
        # Start from tommorrow in case of timezone weirdness causing a path for tomorrow is created before we think
        # it is tommorrow
        start_date = current_date + datetime.timedelta(days=1)
        for age in range(age_in_days+1):
            d = start_date - datetime.timedelta(days=age)
            date_path = self.config.data_root / Path(d.strftime("%Y-%m/%d"))
            paths.add(IngestWatcher.PathInfo(date_path, False))
            paths.add(IngestWatcher.PathInfo(date_path.parent, False))
            for instrument in self.config.instrument_dirs:
                paths.add(IngestWatcher.PathInfo(date_path / instrument, True))

        return paths

    def _watch(self, path_info):
        if path_info.path.exists():
            self.logger.info(f"Watching path {path_info.path}")
            if path_info.watch is not None:
                path_info.observer.unschedule(path_info.watch)
            path_info.watch = path_info.observer.schedule(self, path_info.path)
        else:
            self.logger.info(f"Path {path_info.path} does not exist (yet). Will wait for it to be created.")

    def start(self):
        self.logger.debug(f"Starting...")
        with self._lock:
            for observer in self._observer_list:
                observer.start()
        self.logger.debug(f"Started.")

    def stop(self):
        self.logger.debug(f"Stopping...")
        with self._lock:
            for observer in self._observer_list:                
                observer.stop()
                observer.join()
        self.logger.debug(f"Stopped...")

    def is_alive(self):
        self.logger.debug(f"is_alive...")
        with self._lock:
            result =  all([o.is_alive() for o in self._observer_list])
            self.logger.debug(f"Alive: {result}")
            return result

    def on_any_event(self, event):
        self.logger.debug(repr(event))

    def on_created(self, event):

        try:
            if event.is_directory:
                self.logger.debug("New directory created " + event.src_path)
                resync_paths = []
                # A new directory, is it one we're waiting for?
                with self._lock:
                    # When directories are created together (as in mkdir -p) we won't always get the
                    # create for the lower level directory. So we ignore the source in the event and just
                    # check to see if any of the directories we want to watch now exist
                    for path_info in self._path_info_map.values():
                        self.logger.debug(f"pi.path {path_info.path} pi.watch {path_info.watch}")
                        if path_info.watch is None and path_info.path.exists():
                            self._watch(path_info)
                            if path_info.is_ingest_dir:
                                resync_paths.append(path_info.path)

                for path in resync_paths:
                    self.resync_path(path)
        except Exception as e:
            self.logger.debug("Exception in on_created", exc_info=True)
            raise

    def on_closed(self, event):
        # A new file is done writing, is it in one of our lowest level paths?
        new_file = Path(event.src_path)
        notify = False
        with self._lock:
            if new_file.parent in self._path_info_map:
                notify = self._path_info_map[new_file.parent].is_ingest_dir

        if notify:
            try:
                self.archive_client.ingest_new_files(new_file)
            except RequestException as e:
                logger.error(f"Failed to ingest {new_file} due to failure contacting archive server: {e}")


    def on_moved(self, event):
        # check for a file moved into our lowest level paths
        if event.dest_path is None:
            # We'll get move-in and move-out events some times. move-out only that the source,
            # and we only care about the destination. So we reutrn if there is no dest path
            return
        if not event.is_directory:
            new_file = Path(event.dest_path)
            notify = False
            with self._lock:
                if new_file.parent in self._path_info_map:
                    notify = self._path_info_map[new_file.parent].is_ingest_dir

            if notify:
                try:
                    self.archive_client.ingest_new_files(new_file)
                except RequestException as e:
                    logger.error(f"Failed to ingest {new_file} due to failure contacting archive server: {e}")
        else:
            new_path = Path(event.dest_path)
            resync = False
            with self._lock:
                if new_path in self._path_info_map:                
                    path_info = self._path_info_map[new_path]

                    # A directory we want to watch was  was moved into the archive. Weird but we can support it.
                    if path_info.watch is None:
                        self._watch(path_info)
                        if path_info is not None and path_info.is_ingest_dir:
                            resync=True
            
            if resync:
                self.resync_path(new_path)

    def resync(self, current_date, age):
        for path_info in self._get_paths_for_age(current_date, age):
            if path_info.is_ingest_dir and path_info.path.exists():
                self.resync_path(path_info.path)
            
    def resync_path(self, path):
        self.logger.info(f"Resyncing {path}")
        date = datetime.date.fromisoformat(f"{path.parent.parent.name}-{path.parent.name}")
        instrument_dir = path.name
        archive_count = 0
        try:
            archive_count = self.archive_client.sync_query(date, instrument_dir)
        except RequestException as e:
            logging.error(f"Failed to resync {path} due to failure querying archive server: {e}")


        actual_count = 0
        files_to_sync = []
        for child in path.iterdir():
            if child.is_file():
                files_to_sync.append(child)
                actual_count += 1

        if actual_count > archive_count:
            # We need to resync.
            try:
                self.archive_client.ingest_new_files(files_to_sync)
            except RequestException as e:
                logging.error(f"Failed to resync {path} due to failure ingesting into archive server: {e}")
        


def get_parser():
    """
    Parse bulk_ingest_metadata command line arguments with argparse.
    """
    parser = argparse.ArgumentParser(description='Ingest metadata for Lick data into the archive database.\n'
                                                 'A log file of the ingest is created in bulk_ingest_<timestamp>.log.\n'
                                                 'A separate ingest_failures.n.txt is also created listing files that failed ingesting.')
    parser.add_argument("--config", type=str, default='/etc/lick_archive.conf', help='Config file to read. Defaults to /etc/lick_archive.conf.')
    parser.add_argument("--log_path", "-l", type=Path, default=Path.cwd(), help="Directory to write log file to. Defaults to current directory" )
    parser.add_argument("--log_level", "-L", type=str, choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], default="DEBUG", help="Logging level to use.")
    return parser


def main(args):

    setup_service_logging(args.log_path, "ingest_watchdog", args.log_level, rollover_days=1, backup_count=14, log_tid=True)

    try:
        logger.info(f"Reading configuration from '{args.config}'.")
        config_parser = configparser.ConfigParser()
        config_parser.read(args.config)

        config = parse_and_validate_config(config_parser)

    except ValueError as e:
        logger.error(e)
        return 1

    try:
        client = LickArchiveIngestClient(config.ingest.url, config.ingest.retry_max_delay, config.ingest.retry_max_time, config.ingest.request_timeout)
        watcher = IngestWatcher(config, logger, client)

        current_date = datetime.date.today()
        watcher.start_observers(current_date)
        watcher.resync(current_date, config.startup_resync_age)

        done = False
        while not done:

            # Reset again when the date changes
            while datetime.date.today() == current_date and watcher.is_alive():
                sleep(5)

            if not watcher.is_alive():
                # Something happened to the observer threads, probably a signal, so its time to exit
                done = True

            watcher.stop()
            if not done:
                current_date = datetime.date.today()
                watcher.restart_observers(current_date)

    except Exception as e:        
        logger.critical(f"ingest_watchdog failed with exception: {e}", exc_info=True)
        return 1

    return 0

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    sys.exit(main(args))
