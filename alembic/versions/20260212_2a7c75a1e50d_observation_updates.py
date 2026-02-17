"""Add photo position

Revision ID: 2a7c75a1e50d
Revises: 1085cbe39943
Create Date: 2026-02-16 23:11:22.323874

"""

from logging import getLogger

import sqlalchemy as sa

from alembic import op

logger = getLogger('alembic.runtime.migration')

revision: str = '2a7c75a1e50d'
down_revision = '1085cbe39943'
branch_labels = None
depends_on = None


def upgrade():

    existing_columns = [col['name'] for col in sa.inspect(op.get_bind()).get_columns('observation')]
    if 'created_at' not in existing_columns:
        op.add_column('observation', sa.Column('created_at', sa.String(), nullable=True))
    if 'identifications_count' not in existing_columns:
        op.add_column(
            'observation', sa.Column('identifications_count', sa.Integer(), nullable=True)
        )
    if 'annotations' not in existing_columns:
        op.add_column('observation', sa.Column('annotations', sa.JSON(), nullable=True))
    if 'comments' not in existing_columns:
        op.add_column('observation', sa.Column('comments', sa.JSON(), nullable=True))
    if 'identifications' not in existing_columns:
        op.add_column('observation', sa.Column('identifications', sa.JSON(), nullable=True))
    if 'ofvs' not in existing_columns:
        op.add_column('observation', sa.Column('ofvs', sa.JSON(), nullable=True))
    if 'tags' not in existing_columns:
        op.add_column('observation', sa.Column('tags', sa.String(), nullable=True))

    existing_indexes = [idx['name'] for idx in sa.inspect(op.get_bind()).get_indexes('observation')]
    if 'ix_observation_created_at' not in existing_indexes:
        op.create_index(
            op.f('ix_observation_created_at'), 'observation', ['created_at'], unique=False
        )

    existing_columns = [col['name'] for col in sa.inspect(op.get_bind()).get_columns('taxon')]
    if 'active' in existing_columns:
        op.drop_column('taxon', 'active')
    if 'is_active' not in existing_columns:
        op.add_column('taxon', sa.Column('is_active', sa.Boolean(), nullable=True))
    if 'observations_count_rg' not in existing_columns:
        op.add_column('taxon', sa.Column('observations_count_rg', sa.Integer(), nullable=True))
    if 'reference_url' not in existing_columns:
        op.add_column('taxon', sa.Column('reference_url', sa.String(), nullable=True))


def downgrade():
    op.add_column('taxon', sa.Column('active', sa.BOOLEAN(), nullable=True))
    op.drop_column('taxon', 'reference_url')
    op.drop_column('taxon', 'observations_count_rg')
    op.drop_column('taxon', 'is_active')
    op.drop_index(op.f('ix_observation_created_at'), table_name='observation')
    op.drop_column('observation', 'tags')
    op.drop_column('observation', 'ofvs')
    op.drop_column('observation', 'identifications')
    op.drop_column('observation', 'comments')
    op.drop_column('observation', 'annotations')
    op.drop_column('observation', 'identifications_count')
    op.drop_column('observation', 'created_at')
