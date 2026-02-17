"""initial schema

Revision ID: 1085cbe39943
Revises:
Create Date: 2026-02-16 23:10:29.112189

"""

import sqlalchemy as sa

from alembic import op

revision: str = '1085cbe39943'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'taxon',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('active', sa.Boolean(), nullable=True),
        sa.Column('ancestor_ids', sa.String(), nullable=True),
        sa.Column('child_ids', sa.String(), nullable=True),
        sa.Column('iconic_taxon_id', sa.Integer(), nullable=True),
        sa.Column('leaf_taxa_count', sa.Integer(), nullable=True),
        sa.Column('observations_count', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('parent_id', sa.Integer(), nullable=True),
        sa.Column('partial', sa.Boolean(), nullable=True),
        sa.Column('photo_urls', sa.String(), nullable=True),
        sa.Column('preferred_common_name', sa.String(), nullable=True),
        sa.Column('rank', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ['parent_id'],
            ['taxon.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_taxon_name'), 'taxon', ['name'], unique=False)
    op.create_index(op.f('ix_taxon_parent_id'), 'taxon', ['parent_id'], unique=False)
    op.create_table(
        'user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('login', sa.String(), nullable=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'observation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('captive', sa.Boolean(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('geoprivacy', sa.String(), nullable=True),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.Column('license_code', sa.String(), nullable=True),
        sa.Column('observed_on', sa.String(), nullable=True),
        sa.Column('place_guess', sa.String(), nullable=True),
        sa.Column('place_ids', sa.String(), nullable=True),
        sa.Column('positional_accuracy', sa.Integer(), nullable=True),
        sa.Column('quality_grade', sa.String(), nullable=True),
        sa.Column('taxon_id', sa.Integer(), nullable=True),
        sa.Column('updated_at', sa.String(), nullable=True),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('user_login', sa.Integer(), nullable=True),
        sa.Column('uuid', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ['taxon_id'],
            ['taxon.id'],
        ),
        sa.ForeignKeyConstraint(
            ['user_id'],
            ['user.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_observation_captive'), 'observation', ['captive'], unique=False)
    op.create_index(op.f('ix_observation_geoprivacy'), 'observation', ['geoprivacy'], unique=False)
    op.create_index(
        op.f('ix_observation_observed_on'), 'observation', ['observed_on'], unique=False
    )
    op.create_index(
        op.f('ix_observation_quality_grade'), 'observation', ['quality_grade'], unique=False
    )
    op.create_index(op.f('ix_observation_taxon_id'), 'observation', ['taxon_id'], unique=False)
    op.create_index(op.f('ix_observation_updated_at'), 'observation', ['updated_at'], unique=False)
    op.create_index(op.f('ix_observation_uuid'), 'observation', ['uuid'], unique=False)
    op.create_table(
        'photo',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('extension', sa.String(), nullable=True),
        sa.Column('height', sa.Integer(), nullable=True),
        sa.Column('license', sa.String(), nullable=True),
        sa.Column('observation_id', sa.Integer(), nullable=True),
        sa.Column('observation_uuid', sa.String(), nullable=True),
        sa.Column('url', sa.String(), nullable=True),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('width', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ['observation_id'],
            ['observation.id'],
        ),
        sa.ForeignKeyConstraint(
            ['observation_uuid'],
            ['observation.uuid'],
        ),
        sa.ForeignKeyConstraint(
            ['user_id'],
            ['user.id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_photo_observation_id'), 'photo', ['observation_id'], unique=False)
    op.create_index(op.f('ix_photo_observation_uuid'), 'photo', ['observation_uuid'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_photo_observation_uuid'), table_name='photo')
    op.drop_index(op.f('ix_photo_observation_id'), table_name='photo')
    op.drop_table('photo')
    op.drop_index(op.f('ix_observation_uuid'), table_name='observation')
    op.drop_index(op.f('ix_observation_updated_at'), table_name='observation')
    op.drop_index(op.f('ix_observation_taxon_id'), table_name='observation')
    op.drop_index(op.f('ix_observation_quality_grade'), table_name='observation')
    op.drop_index(op.f('ix_observation_observed_on'), table_name='observation')
    op.drop_index(op.f('ix_observation_geoprivacy'), table_name='observation')
    op.drop_index(op.f('ix_observation_captive'), table_name='observation')
    op.drop_table('observation')
    op.drop_table('user')
    op.drop_index(op.f('ix_taxon_parent_id'), table_name='taxon')
    op.drop_index(op.f('ix_taxon_name'), table_name='taxon')
    op.drop_table('taxon')
