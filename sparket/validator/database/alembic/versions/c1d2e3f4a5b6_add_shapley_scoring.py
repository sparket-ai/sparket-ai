"""add_shapley_scoring

Add columns to miner_rolling_score for Cobb-Douglas 5-pillar scoring:
uniqueness_dim, marginal_dim, composite SOS components, Shapley accumulators,
and cluster assignment. Also creates new tables for pairwise correlation
and per-market Shapley contributions.

Revision ID: c1d2e3f4a5b6
Revises: b7c8d9e0f1a2
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c1d2e3f4a5b6'
down_revision = 'b7c8d9e0f1a2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # New columns on miner_rolling_score for Cobb-Douglas pillars
    op.add_column('miner_rolling_score', sa.Column(
        'uniqueness_dim', sa.Numeric(), nullable=True,
        comment='Uniqueness dimension = composite SOS [0,1]',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'marginal_dim', sa.Numeric(), nullable=True,
        comment='Marginal dimension = normalised Shapley contribution [0,1]',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'sos_crowd', sa.Numeric(), nullable=True,
        comment='Mean pairwise miner correlation score (1 - mean|corr|)',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'sos_cluster', sa.Numeric(), nullable=True,
        comment='Cluster penalty score (1 - ClusterPenalty)',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'sos_composite', sa.Numeric(), nullable=True,
        comment='Weighted blend: 0.2*market + 0.5*crowd + 0.3*cluster',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'shapley_mean', sa.Numeric(), nullable=True,
        comment='Time-decayed rolling Shapley value',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'shapley_ws', sa.Numeric(), nullable=True,
        comment='Shapley accumulator weighted sum',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'shapley_wt', sa.Numeric(), nullable=True,
        comment='Shapley accumulator weight total',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'cluster_id', sa.Integer(), nullable=True,
        comment='Assigned cluster ID from spectral clustering',
    ))
    op.add_column('miner_rolling_score', sa.Column(
        'cluster_size', sa.Integer(), nullable=True, server_default='1',
        comment='Size of assigned cluster (1 = singleton)',
    ))

    # New table: miner_pairwise_correlation
    op.create_table(
        'miner_pairwise_correlation',
        sa.Column('miner_a_id', sa.BigInteger(), nullable=False),
        sa.Column('miner_b_id', sa.BigInteger(), nullable=False),
        sa.Column('as_of', sa.DateTime(timezone=True), nullable=False),
        sa.Column('correlation', sa.Numeric(), nullable=False,
                   comment='Pearson correlation between miner submissions'),
        sa.Column('n_common_markets', sa.Integer(), nullable=False,
                   comment='Number of overlapping market submissions'),
        sa.PrimaryKeyConstraint('miner_a_id', 'miner_b_id', 'as_of'),
    )
    op.create_index(
        'ix_miner_pairwise_corr_as_of',
        'miner_pairwise_correlation',
        ['as_of'],
    )

    # New table: shapley_contribution
    op.create_table(
        'shapley_contribution',
        sa.Column('miner_id', sa.BigInteger(), nullable=False),
        sa.Column('market_id', sa.Integer(), nullable=False),
        sa.Column('shapley_value', sa.Numeric(), nullable=False,
                   comment='Raw Shapley value for this market'),
        sa.Column('settled_at', sa.DateTime(timezone=True), nullable=False,
                   comment='When the underlying market settled'),
        sa.Column('k_permutations', sa.Integer(), nullable=False,
                   comment='Number of Monte Carlo permutations used'),
        sa.PrimaryKeyConstraint('miner_id', 'market_id'),
    )
    op.create_index(
        'ix_shapley_contribution_settled',
        'shapley_contribution',
        ['settled_at'],
    )


def downgrade() -> None:
    op.drop_table('shapley_contribution')
    op.drop_table('miner_pairwise_correlation')
    op.drop_column('miner_rolling_score', 'cluster_size')
    op.drop_column('miner_rolling_score', 'cluster_id')
    op.drop_column('miner_rolling_score', 'shapley_wt')
    op.drop_column('miner_rolling_score', 'shapley_ws')
    op.drop_column('miner_rolling_score', 'shapley_mean')
    op.drop_column('miner_rolling_score', 'sos_composite')
    op.drop_column('miner_rolling_score', 'sos_cluster')
    op.drop_column('miner_rolling_score', 'sos_crowd')
    op.drop_column('miner_rolling_score', 'marginal_dim')
    op.drop_column('miner_rolling_score', 'uniqueness_dim')
