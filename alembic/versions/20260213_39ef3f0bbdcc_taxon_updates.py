"""Taxon updates

# Revision ID: 39ef3f0bbdcc
Revises: 2a7c75a1e50d
Create Date: 2026-02-16 23:12:10.398331

"""

import sqlalchemy as sa

from alembic import op

revision: str = '39ef3f0bbdcc'
down_revision = '2a7c75a1e50d'
branch_labels = None
depends_on = None


def upgrade():
    existing_columns = [col['name'] for col in sa.inspect(op.get_bind()).get_columns('taxon')]
    if 'conservation_status' not in existing_columns:
        op.add_column('taxon', sa.Column('conservation_status', sa.String(), nullable=True))
    if 'establishment_means' not in existing_columns:
        op.add_column('taxon', sa.Column('establishment_means', sa.String(), nullable=True))
    if 'wikipedia_summary' not in existing_columns:
        op.add_column('taxon', sa.Column('wikipedia_summary', sa.String(), nullable=True))
    if 'wikipedia_url' not in existing_columns:
        op.add_column('taxon', sa.Column('wikipedia_url', sa.String(), nullable=True))


def downgrade():
    op.drop_column('taxon', 'wikipedia_url')
    op.drop_column('taxon', 'wikipedia_summary')
    op.drop_column('taxon', 'establishment_means')
    op.drop_column('taxon', 'conservation_status')
