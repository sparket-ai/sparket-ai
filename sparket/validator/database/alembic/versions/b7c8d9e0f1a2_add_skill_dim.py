"""add_skill_dim

Add skill_dim column to miner_rolling_score for persisting the
SkillDim (PSS_norm) dimension alongside the existing forecast_dim,
econ_dim, and info_dim columns.

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2026-02-10 00:00:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b7c8d9e0f1a2'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('miner_rolling_score', sa.Column(
        'skill_dim', sa.Numeric(), nullable=True,
        comment='Skill dimension = PSS_norm (outcome relative skill)',
    ))


def downgrade() -> None:
    op.drop_column('miner_rolling_score', 'skill_dim')
