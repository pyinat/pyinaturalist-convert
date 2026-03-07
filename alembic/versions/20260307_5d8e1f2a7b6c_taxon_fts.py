"""Add taxon FTS table and sync triggers

Revision ID: 5d8e1f2a7b6c
Revises: c7d8a2f88e3f
Create Date: 2026-03-07 00:00:00.000000

"""

from logging import getLogger

import sqlalchemy as sa

from alembic import op
from pyinaturalist_convert.fts import (
    TAXON_FTS_TABLE,
    _create_taxon_fts_table_sql,
    _create_taxon_fts_trigger_sql,
)

revision: str = '5d8e1f2a7b6c'
down_revision = 'c7d8a2f88e3f'
branch_labels = None
depends_on = None

logger = getLogger('alembic.runtime.migration')
TAXON_FTS_TRIGGER_NAMES = ('taxon_ai', 'taxon_au', 'taxon_ad')


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    existing_tables = set(sa.inspect(bind).get_table_names())
    if TAXON_FTS_TABLE in existing_tables:
        logger.info("'%s' already exists; ensuring triggers exist", TAXON_FTS_TABLE)
    else:
        bind.exec_driver_sql(_create_taxon_fts_table_sql())
        logger.info("Created '%s' table", TAXON_FTS_TABLE)

    for trigger_name, sql in zip(
        TAXON_FTS_TRIGGER_NAMES, _create_taxon_fts_trigger_sql(), strict=False
    ):
        bind.exec_driver_sql(sql)
        logger.info("Created trigger '%s'", trigger_name)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    for trigger in TAXON_FTS_TRIGGER_NAMES:
        bind.exec_driver_sql(f'DROP TRIGGER IF EXISTS {trigger}')
    bind.exec_driver_sql(f'DROP TABLE IF EXISTS {TAXON_FTS_TABLE}')
