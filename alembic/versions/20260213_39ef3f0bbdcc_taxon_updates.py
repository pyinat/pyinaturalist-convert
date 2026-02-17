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
    op.add_column('taxon', sa.Column('conservation_status', sa.String(), nullable=True))
    op.add_column('taxon', sa.Column('establishment_means', sa.String(), nullable=True))
    op.add_column('taxon', sa.Column('wikipedia_summary', sa.String(), nullable=True))
    op.add_column('taxon', sa.Column('wikipedia_url', sa.String(), nullable=True))


def downgrade():
    op.drop_column('taxon', 'wikipedia_url')
    op.drop_column('taxon', 'wikipedia_summary')
    op.drop_column('taxon', 'establishment_means')
    op.drop_column('taxon', 'conservation_status')
