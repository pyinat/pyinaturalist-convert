"""Add observation FTS table and sync triggers

Revision ID: c7d8a2f88e3f
Revises: 486206627704
Create Date: 2026-03-06 21:30:00.000000

"""

from logging import getLogger

from alembic import op
from pyinaturalist_convert.fts import (
    OBS_FTS_TABLE,
    _create_observation_fts_table_sql,
    _create_observation_fts_trigger_sql,
    _downgrade_fts_sync,
    _upgrade_fts_sync,
)

revision: str = 'c7d8a2f88e3f'
down_revision = '486206627704'
branch_labels = None
depends_on = None

logger = getLogger('alembic.runtime.migration')


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    _upgrade_fts_sync(
        bind,
        OBS_FTS_TABLE,
        _create_observation_fts_table_sql(),
        _create_observation_fts_trigger_sql(),
        logger,
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    _downgrade_fts_sync(bind, _create_observation_fts_trigger_sql())
