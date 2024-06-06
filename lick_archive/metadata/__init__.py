"""Package responsible for reading metadata from files in the Lick Archive"""
from sqlalchemy import Table
from typing import TypeAlias
from lick_archive.db.archive_schema import Main, UserDataAccess

# Define the base type for metadata in the archive. We use an alias
# just to make it easy to change away from SQLAlchemy if needed
# in the future
Metadata: TypeAlias = Table
"""Base type for metadata in the lick archive."""

FileMetadata : TypeAlias = Main
"""Metadata type for files in the lick archive."""

UserAccessMetadata : TypeAlias = UserDataAccess