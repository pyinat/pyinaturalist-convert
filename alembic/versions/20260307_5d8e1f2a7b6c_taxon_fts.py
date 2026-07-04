"""Add taxon FTS table and sync triggers

Revision ID: 5d8e1f2a7b6c
Revises: c7d8a2f88e3f
Create Date: 2026-03-07 00:00:00.000000

"""

from logging import getLogger

from alembic import op
from pyinaturalist_convert.fts import (
    TAXON_FTS_TABLE,
    _create_taxon_fts_table_sql,
    _create_taxon_fts_trigger_sql,
    _downgrade_fts_sync,
    _upgrade_fts_sync,
)

revision: str = '5d8e1f2a7b6c'
down_revision = 'c7d8a2f88e3f'
branch_labels = None
depends_on = None

logger = getLogger('alembic.runtime.migration')


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    _upgrade_fts_sync(
        bind,
        TAXON_FTS_TABLE,
        _create_taxon_fts_table_sql(),
        _create_taxon_fts_trigger_sql(),
        logger,
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    _downgrade_fts_sync(bind, _create_taxon_fts_trigger_sql())
