"""create processed files table

Revision ID: bdbb7212ffb6
Revises: d6b4393c5b33
Create Date: 2025-04-27 19:03:41.217541

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bdbb7212ffb6'
down_revision: Union[str, None] = 'd6b4393c5b33'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'processed_files',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('file_name', sa.String(), nullable=False),
        sa.Column('processed_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('file_name')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('processed_files')
