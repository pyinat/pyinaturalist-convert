"""Add photo file path + original filename + uuid + position

Revision ID: 486206627704
Revises: 39ef3f0bbdcc
Create Date: 2026-02-16 23:12:52.694619

"""

import sqlalchemy as sa

from alembic import op

revision: str = '486206627704'
down_revision = '39ef3f0bbdcc'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('photo', sa.Column('file_path', sa.String(), nullable=True))
    op.add_column('photo', sa.Column('original_filename', sa.String(), nullable=True))
    op.add_column('photo', sa.Column('position', sa.Integer(), nullable=True))
    op.add_column('photo', sa.Column('uuid', sa.String(), nullable=True))


def downgrade():
    op.drop_column('photo', 'original_filename')
    op.drop_column('photo', 'file_path')
    op.drop_column('photo', 'uuid')
    op.drop_column('photo', 'position')
