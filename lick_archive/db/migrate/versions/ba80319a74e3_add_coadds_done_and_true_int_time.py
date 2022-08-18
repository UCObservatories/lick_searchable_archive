"""Add coadds_done, true_int_time, and ingest_flags

Revision ID: ba80319a74e3
Revises: 
Create Date: 2022-06-23 20:53:36.325789+00:00

"""
from pathlib import Path
from collections import namedtuple

from alembic import op
from sqlalchemy import select, update
from sqlalchemy import Column, Float, Integer
from sqlalchemy.dialects.postgresql import BIT

from astropy.io import fits

from lick_archive.db.archive_schema import Main
from ingest.reader import read_row, read_hdul

# revision identifiers, used by Alembic.
revision = 'ba80319a74e3'
down_revision = None
branch_labels = None
depends_on = None

BATCH_SIZE = 10000

def upgrade() -> None:
    op.add_column('main', Column('coadds_done', Integer))
    op.add_column('main', Column('true_int_time', Float))
    op.add_column('main', Column('ingest_flags', BIT(32)))

    FakeHDUL = namedtuple('FakeHDUL', ['header'])

    connection = op.get_bind()
    last_id = 0
    done = False
    while(done is False):
        result = connection.execute(select(Main.id, Main.filename, Main.header).
                                    where(Main.id > last_id).
                                    execution_options(yield_per=BATCH_SIZE))
        rows = result.fetchmany(BATCH_SIZE)
        # Close the result to make sure any locks
        # on main are released
        result.close()
        if len(rows) == 0:
            done = True

        for (id, filename, header) in rows:
            hdul = [FakeHDUL(fits.Header.fromstring(header, sep='\n'))]
            #new_row = read_row(Path(filename))
            new_row = read_hdul(Path(filename), hdul)
            op.execute(update(Main).
                       values(coadds_done = new_row.coadds_done,
                              true_int_time = new_row.true_int_time,
                              ingest_flags = new_row.ingest_flags).
                       where(Main.id == id))
            last_id = id




def downgrade() -> None:
    op.drop_column('main', 'coadds_done')
    op.drop_column('main', 'true_int_time')
    op.drop_column('main', 'ingest_flags')
