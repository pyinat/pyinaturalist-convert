"""Add observation FTS table and sync triggers

Revision ID: c7d8a2f88e3f
Revises: 486206627704
Create Date: 2026-03-06 21:30:00.000000

"""

from logging import getLogger

import sqlalchemy as sa

from alembic import op
from pyinaturalist_convert.fts import (
    OBS_FTS_TABLE,
    _create_observation_fts_table_sql,
    _create_observation_fts_trigger_sql,
)

revision: str = 'c7d8a2f88e3f'
down_revision = '486206627704'
branch_labels = None
depends_on = None

logger = getLogger('alembic.runtime.migration')
OBS_FTS_TRIGGER_NAMES = ('observation_ai', 'observation_au', 'observation_ad')


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    existing_tables = set(sa.inspect(bind).get_table_names())
    if OBS_FTS_TABLE in existing_tables:
        logger.info("'%s' already exists; ensuring triggers exist", OBS_FTS_TABLE)
    else:
        bind.exec_driver_sql(_create_observation_fts_table_sql())
        logger.info("Created '%s' table", OBS_FTS_TABLE)

    for trigger_name, sql in zip(
        OBS_FTS_TRIGGER_NAMES, _create_observation_fts_trigger_sql(), strict=False
    ):
        bind.exec_driver_sql(sql)
        logger.info("Created trigger '%s'", trigger_name)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != 'sqlite':
        return

    for trigger in OBS_FTS_TRIGGER_NAMES:
        bind.exec_driver_sql(f'DROP TRIGGER IF EXISTS {trigger}')
    bind.exec_driver_sql(f'DROP TABLE IF EXISTS {OBS_FTS_TABLE}')
