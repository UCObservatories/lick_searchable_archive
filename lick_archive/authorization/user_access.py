import logging

logger = logging.getLogger(__name__)
import dataclasses
from enum import Enum
from pathlib import Path
from datetime import date, datetime, timezone
from typing import Sequence
import re

from lick_archive.db.archive_schema import FileMetadata, UserDataAccess
from lick_archive.authorization import override_access
from lick_archive.authorization.date_utils import get_file_begin_end_times, get_observing_night, calculate_public_date
from lick_archive.data_dictionary import FrameType, MAX_PUBLIC_DATE
from lick_external import ScheduleDB, compute_ownerhints, get_keyword_ownerhints


from lick_archive.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

class Visibility(Enum):
    """The access visibility granted by an override access file "access" rule."""
    PUBLIC         = "Public"
    PROPRIETARY    = "Proprietary"
    UNKNOWN        = "Unknown"
    DEFAULT        = "DEFAULT"

@dataclasses.dataclass
class Access:
    """Class defining the access state of the file, i.e. who can see it and why."""

    observing_night: date
    """The observing night for the file, based on Pacific Standard Time (UTC-8)"""

    file_metadata : FileMetadata
    """The file metadata for the file"""

    visibility: Visibility
    """The file's visibility"""

    ownerids: list[int]
    """The observer id for the owners for the file"""

    coverids: list[str]
    """The coversheet ids for the file"""

    reason: list[str]
    """Why the file ended up with the access state it has"""

    public_date: date = None
    """The date the file becomes public."""

def reason(rule, description):
    return f"Rule {rule}: {description}"

def set_auth_metadata(file_metadata : FileMetadata) -> FileMetadata:
    access = identify_access(file_metadata)

    if access.visibility==Visibility.PROPRIETARY:
        # Set the public dates for the file if it's proprietary
        public_dates = ScheduleDB().get_public_dates(file_metadata.telescope, access.observing_night, access.ownerids)

        # Add default dates to public_dates if one isn't set
        default_public_date = calculate_public_date(access.observing_night, lick_archive_config.authorization.default_proprietary_period)
        public_dates = [(obid, d, False) if d is not None else (obid, default_public_date, True) for obid, d in public_dates]
        public_dates.sort(key=lambda x : x[1])

        obid, earliest_date, is_default = public_dates[0]

        if get_observing_night(datetime.now(tz=timezone.utc)) >= earliest_date:
            # The file is public now
            access.visibility=Visibility.PUBLIC
            if is_default is False:
                access.reason.append(reason("0", f"File has passed observer {obid}'s proprietary end date of {earliest_date}"))
            else:
                access.reason.append(reason("0", f"File has passed default proprietary end date of {earliest_date}"))
        else:
            access.reason.append(reason("0", f"File is not public, earliest public date is {earliest_date} from observer {obid}."))
            access.public_date = earliest_date

    elif access.visibility==Visibility.DEFAULT:
        # Rule 6 (called 7 in old Rules.txt). No observers could be found for the file,
        # so it's public
        access.visibility=Visibility.PUBLIC
        access.reason.append(reason("6", f"No observers found for file"))

    return set_access_metadata(file_metadata, access)

def set_access_metadata(file_metadata : FileMetadata, access : Access) -> FileMetadata:
    
    if access.coverids is not None and len(access.coverids) > 0:
        file_metadata.coversheet = ";".join(access.coverids)

    if access.visibility == Visibility.PUBLIC:
        # Public, treat it as if it became public the date it was taken
        file_metadata.public_date = access.observing_night
    elif access.visibility == Visibility.UNKNOWN:
        # Unknown should always have max public date
        file_metadata.public_date = MAX_PUBLIC_DATE
    elif access.public_date is not None:
        file_metadata.public_date = access.public_date
    else:        
        # No publication date and not PUBLIC or UNKNOWN.
        # Treat it as UNKNOWN
        file_metadata.public_date = MAX_PUBLIC_DATE
        access.visibility = Visibility.UNKNOWN

    # Make sure unknown files have the UNKNOWN user as their owner
    if access.visibility == Visibility.UNKNOWN and ScheduleDB.UNKNOWN_USER not in access.ownerids:
        access.ownerids.append(ScheduleDB.UNKNOWN_USER)

    reason_string = "\n".join(access.reason)
    for ownerid in access.ownerids:
        file_metadata.user_access.append(UserDataAccess(obid=ownerid, reason = reason_string))

    logger.info(f"Setting access metadata for {file_metadata.filename}. Public date: {file_metadata.public_date}\nReason:\n{reason_string}")
    return file_metadata

def identify_access(file_metadata : FileMetadata) -> Access:

    # The access data for the file, defaulting to "UNKNOWN"
    access = Access(file_metadata = file_metadata,
                    observing_night=get_observing_night(file_metadata.obs_date),
                    visibility = Visibility.DEFAULT,
                    ownerids = [], coverids=[],
                    reason = [])

    filepath = Path(file_metadata.filename)
    instr = file_metadata.instrument
    
    # Rule 1: Check for override access rules
    try:
        from archive_auth.models import get_related_override_files
        override_files = get_related_override_files(filepath)
    except Exception as e:
        access.reason.append(reason("1z", "Failed when querying for override access."))
        access.visibility = Visibility.UNKNOWN
        logger.error("Failed to read override access files.", exc_info=True)
        return access
    
    override_rule = override_access.find_matching_rules(override_files, filepath)

    if override_rule is not None:
        # There were rules in the override access file(s) that matched this file
        # Rule 1a: Apply any type overrides
        if override_rule.obstype is not None:                
            # Override the database's frame type with the new value.
            file_metadata.frame_type = override_rule.obstype

            if override_rule.obstype not in [FrameType.science, FrameType.unknown]:
                access.reason.append(reason("1a", f"All observers from the night included because override access set file type to {override_rule.obstype.value}."))
                apply_ownerhints(access, "1a", ["all-observers"])
            else:
                access.reason.append(reason("1a", f"No special rule for obstype: {override_rule.obstype.value}"))

        # Rule 1b,c,d: Apply any ownerhints from the override access
        if len(override_rule.ownerhints) > 0:               
            if "public" in override_rule.ownerhints:
                access.visibility = Visibility.PUBLIC
                access.reason.append(reason("1b", f"Override access file gave public visibility."))
            else:
                apply_ownerhints(access, "1b/c/d", override_rule.ownerhints)

        if access.visibility != Visibility.DEFAULT:
            # The override access had enough information to set the access, so return those results
            return access

            
    # Rule 2a: Check for always public files
    
    public_suffixes = lick_archive_config.authorization.public_suffixes[instr.value]
    for suffix in public_suffixes:
        if filepath.name.endswith(suffix):
            access.visibility = Visibility.PUBLIC
            access.reason.append(reason("2a", f"Suffix {suffix} is public for instrument: {instr.value}"))
            return access
    
    # Rule 2b: Check for fixed owners
    fixed_owner = lick_archive_config.authorization.fixed_owners[instr.value]
    if fixed_owner is not None:
        if fixed_owner in lick_archive_config.authorization.public_observers:
            access.visibility = Visibility.PUBLIC
            access.reason.append(reason("2b", f"Fixed public owner {fixed_owner} for instrument {instr.value}."))
        else:
            apply_ownerhints(access, "2b", fixed_owner)

            if access.visibility == Visibility.DEFAULT:
                # This shouldn't happen, the fixed owner in the config file must not match what apply_ownerhints expects,
                # which is probably an error
                access.visibility=Visibility.UNKNOWN
                access.reason.append(reason("2z", f"Unknown fixed owner {fixed_owner}, this is likely an archive mis-configuration."))
        # The fixed owner determined ownership
        return access

    # Rule 3: Calibration/focus frame type shoud be viewable to all observers on that night
    if file_metadata.frame_type not in [FrameType.science, FrameType.unknown]:
        access.reason.append(reason("3", f"All observers from the night can access frame type: {file_metadata.frame_type.value}"))
        apply_ownerhints(access, "3", ["all-observers"])
        return access

    # Rule 4: Look for ownerhints from the schedule keyword history

    # This returns tuples [time, ownerhint], sorted by time, time is returned as a datetime
    try:
        schedule_ownerhints = get_keyword_ownerhints(file_metadata.telescope, access.observing_night)
    except Exception as e:
        logger.error(f"Failed to query for OWNERHINT for {instr.value} on {access.observing_night.isoformat()}", exc_info=True)
        access.reason.append(reason("4z", f"Failed to query for OWNRHINT for {instr.value} on {access.observing_night.isoformat()}: {e}"))
        access.visibility = Visibility.UNKNOWN
        return access
        
    # Get beg/end_times from the file's header information
    beg_time, end_time = get_file_begin_end_times(file_metadata)

    # 4a: First look for ownerhints between the beginning/end time of the file
    ownerhints = []
    ownerhint_search_rule = "4a"
    if beg_time is not None and end_time is not None:
        ownerhints = [so[1] for so in schedule_ownerhints if beg_time <= so[0] and end_time >= so[0]]

    # 4b: If there is no beg/end times, or nothing was found in the beginning/end times, find the latest entry before the file's mtime
    if len(ownerhints) == 0:       
        ownerhint_search_rule = "4b"
        if file_metadata.mtime is None:
            access.visibility = Visibility.UNKNOWN
            access.reason.append(reason("4v", f"No mtime information in db."))
            return access

        ownerhints = [so[1] for so in schedule_ownerhints if so[0] < file_metadata.mtime]
        if len(ownerhints) > 1:
            ownerhints = [ownerhints[-1]]

    if len(ownerhints) == 1:
        apply_ownerhints(access, ownerhint_search_rule, ownerhints)
        
        if access.visibility == Visibility.DEFAULT:
            access.visibility = Visibility.UNKNOWN
            access.reason.append(reason("4y", f"No owner found for ownerhint: {ownerhints[0]}"))
            return access

    elif len(ownerhints) > 1:
        access.visibility = Visibility.UNKNOWN
        access.reason.append(reason("4w", f"Multiple ownerhints for file: {','.join(ownerhints)}"))
        return access
    else:
        access.reason.append(reason(ownerhint_search_rule, f"No ownerhints found."))

    # Rule 5 Look for all observers on that night. 
    apply_ownerhints(access, "5", ["all-observers"])
    
    return access
    

def apply_ownerhints(access : Access, rule : str, ownerhints : Sequence[str]):
    """Apply ownerhints to a file to find it's owners"""
    try:
        if "all-observers" in ownerhints:
            allow_multiple=True
        else:
            allow_multiple=False

        # Convert public ownerhints to be "public"
        public_ownerhint_pattern = lick_archive_config.authorization.public_ownerhint_pattern
        if public_ownerhint_pattern is not None:
            ownerhints = [oh if not re.match(public_ownerhint_pattern, oh) else "public" for oh in ownerhints]

        # Query the schedule database for matching observer ids and cover ids
        obids, coverids = compute_ownerhints(access.observing_night, access.file_metadata.telescope, ownerhints)
    except Exception as e:
        logger.error(f"Failed to query schedule db for date {access.observing_night}, telescope: {access.file_metadata.telescope}: {e}", exc_info=True)
        access.reason.append(reason(rule, f"Observing calendar ownerhint query failed: {e}"))
        access.visibility = Visibility.UNKNOWN
        return

    if len(obids)> 0:
        if ScheduleDB.PUBLIC_USER in obids:
            access.visibility = Visibility.PUBLIC
            access.reason.append(reason(rule, "Observing calendar ownerhint query returned public user."))

        elif ScheduleDB.UNKNOWN_USER in obids:
            # Leave visibility at it's default value in case another rule can assign a value
            access.visibility = Visibility.DEFAULT            
            access.reason.append(reason(rule, f"Observing calendar ownerhint query returned unknown user."))
        elif len(obids) > 1 and not allow_multiple:
            access.visibility = Visibility.DEFAULT            
            access.reason.append(reason(rule, f"Observing calendar ownerhint query returned multiple users."))
        else:
            access.ownerids = obids
            access.visibility = Visibility.PROPRIETARY

    if len(coverids) > 0:
        access.coverids = coverids

    access.reason.append(reason(rule, f"Found {len(obids)} observers and {len(coverids)} coverids from override access ownerhints: {','.join(ownerhints)}"))

