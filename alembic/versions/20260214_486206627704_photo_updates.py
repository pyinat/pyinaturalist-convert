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
    existing_columns = [col['name'] for col in sa.inspect(op.get_bind()).get_columns('photo')]
    if 'file_path' not in existing_columns:
        op.add_column('photo', sa.Column('file_path', sa.String(), nullable=True))
    if 'original_filename' not in existing_columns:
        op.add_column('photo', sa.Column('original_filename', sa.String(), nullable=True))
    if 'position' not in existing_columns:
        op.add_column('photo', sa.Column('position', sa.Integer(), nullable=True))
    if 'uuid' not in existing_columns:
        op.add_column('photo', sa.Column('uuid', sa.String(), nullable=True))


def downgrade():
    op.drop_column('photo', 'original_filename')
    op.drop_column('photo', 'file_path')
    op.drop_column('photo', 'uuid')
    op.drop_column('photo', 'position')
