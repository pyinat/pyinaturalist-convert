"""
Unit tests for taxonomy aggregation correctness using synthetic datasets.

Tests verify that aggregate_taxon_db correctly computes:
- ancestor_ids: path from root to each taxon
- child_ids: direct children of each taxon
- iconic_taxon_id: most specific iconic taxon in ancestry
- observations_count_rg: sum of observations in subtree
- leaf_taxa_count: count of leaf taxa in subtree
"""

import sqlite3
from typing import Dict

import pytest

from pyinaturalist_convert.db import create_tables
from pyinaturalist_convert.taxonomy import aggregate_taxon_db
from test.synthetic_taxonomy import (
    SyntheticTaxonomy,
    generate_imbalanced_tree,
    generate_linear_chain,
    generate_synthetic_taxonomy,
    generate_tree_with_iconic_taxa,
    generate_tree_with_mid_leaves,
    generate_wide_shallow_tree,
)


def _load_synthetic_to_db(taxonomy: SyntheticTaxonomy, db_path) -> None:
    """Load synthetic taxonomy into SQLite database for testing."""
    create_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')
        conn.execute('DELETE FROM observation')

        # Insert taxa
        for node in taxonomy.nodes.values():
            conn.execute(
                """INSERT INTO taxon (id, parent_id, name, rank, observations_count_rg, leaf_taxa_count)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    node.id,
                    node.parent_id,
                    node.name,
                    node.rank,
                    0,  # Start with 0, will be computed by aggregate_taxon_db
                    0,
                ),
            )

        # Insert observations (one per taxon with observation_count > 0)
        obs_id = 1
        for node in taxonomy.nodes.values():
            for _ in range(node.observation_count):
                conn.execute(
                    'INSERT INTO observation (id, taxon_id) VALUES (?, ?)',
                    (obs_id, node.id),
                )
                obs_id += 1

        conn.commit()


def _get_results(db_path) -> Dict[int, Dict]:
    """Fetch aggregation results from database."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM taxon').fetchall()
        return {row['id']: dict(row) for row in rows}


def _parse_id_list(value: str | None) -> list[int]:
    """Parse comma-separated ID string to list of ints."""
    if not value:
        return []
    return [int(x) for x in value.split(',') if x]


@pytest.fixture
def db_path(tmp_path):
    """Provide a temporary database path."""
    return tmp_path / 'test_taxonomy.db'


@pytest.fixture
def common_names_path(tmp_path):
    """Provide an empty common names file."""
    path = tmp_path / 'common_names.csv'
    path.write_text('id,vernacularName\n')
    return path


class TestObservationCountAggregation:
    """Tests for observations_count_rg aggregation."""

    def test_simple_tree(self, db_path, common_names_path, tmp_path):
        """Test observation count aggregation on a simple balanced tree."""
        taxonomy = generate_synthetic_taxonomy(
            depth=3,
            branching_factor=2,
            observation_count_range=(1, 1),
            seed=42,
        )

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual_obs = results[node.id]['observations_count_rg']
            expected_obs = node.expected_obs_count
            assert actual_obs == expected_obs, (
                f'Taxon {node.id}: expected obs_count={expected_obs}, got {actual_obs}'
            )

            actual_leaf = results[node.id]['leaf_taxa_count']
            expected_leaf = node.expected_leaf_count
            assert actual_leaf == expected_leaf, (
                f'Taxon {node.id}: expected leaf_count={expected_leaf}, got {actual_leaf}'
            )

    def test_zero_observations(self, db_path, common_names_path, tmp_path):
        """Test that taxa with no observations still aggregate correctly."""
        taxonomy = generate_synthetic_taxonomy(
            depth=3,
            branching_factor=2,
            observation_count_range=(0, 0),
            seed=42,
        )

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual = results[node.id]['observations_count_rg']
            assert actual == 0, f'Taxon {node.id}: expected 0 observations, got {actual}'


class TestAncestorIds:
    """Tests for ancestor_ids computation."""

    def test_ancestor_path(self, db_path, common_names_path, tmp_path):
        """Test that ancestor_ids contains correct path from root."""
        taxonomy = generate_linear_chain(length=5, seed=42)

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual = _parse_id_list(results[node.id]['ancestor_ids'])
            expected = node.expected_ancestor_ids
            assert actual == expected, (
                f'Taxon {node.id}: expected ancestors={expected}, got {actual}'
            )

    def test_root_has_no_ancestors(self, db_path, common_names_path, tmp_path):
        """Test that root taxon has empty ancestor_ids."""
        taxonomy = generate_synthetic_taxonomy(depth=2, branching_factor=2, seed=42)

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)
        root_id = taxonomy.root_id

        ancestors = _parse_id_list(results[root_id]['ancestor_ids'])
        assert ancestors == [], f'Root should have no ancestors, got {ancestors}'


class TestChildIds:
    """Tests for child_ids computation."""

    def test_child_ids(self, db_path, common_names_path, tmp_path):
        """Test that child_ids contains direct children."""
        taxonomy = generate_synthetic_taxonomy(depth=3, branching_factor=2, seed=42)

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual = set(_parse_id_list(results[node.id]['child_ids']))
            expected = set(node.expected_child_ids)
            assert actual == expected, (
                f'Taxon {node.id}: expected children={expected}, got {actual}'
            )

    def test_leaf_has_no_children(self, db_path, common_names_path, tmp_path):
        """Test that leaf taxa have empty child_ids."""
        taxonomy = generate_synthetic_taxonomy(depth=3, branching_factor=2, seed=42)

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        leaves = [n for n in taxonomy.nodes.values() if not n.expected_child_ids]

        for leaf in leaves:
            children = _parse_id_list(results[leaf.id]['child_ids'])
            assert children == [], f'Leaf {leaf.id} should have no children, got {children}'


class TestIconicTaxonId:
    """Tests for iconic_taxon_id assignment."""

    def test_iconic_taxon_propagation(self, db_path, common_names_path, tmp_path):
        """Test that iconic_taxon_id is correctly propagated from ancestors."""
        taxonomy = generate_tree_with_iconic_taxa()

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual = results[node.id]['iconic_taxon_id']
            expected = node.expected_iconic_taxon_id
            assert actual == expected, (
                f'Taxon {node.id} ({node.name}): expected iconic={expected}, got {actual}'
            )

    def test_nested_iconic_taxa(self, db_path, common_names_path, tmp_path):
        """Test that nested iconic taxa use the most specific (deepest) iconic ancestor."""
        taxonomy = generate_tree_with_iconic_taxa()

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        # Aves (id=3) is under Animalia (id=1), both are iconic
        # Aves should have iconic_taxon_id=3 (itself), not 1 (parent)
        assert results[3]['iconic_taxon_id'] == 3, 'Aves should use itself as iconic taxon'

        # Species under Aves should also use Aves (3) as iconic, not Animalia (1)
        assert results[100]['iconic_taxon_id'] == 3, 'Bird species should use Aves as iconic taxon'
        assert results[101]['iconic_taxon_id'] == 3, 'Bird species should use Aves as iconic taxon'

    def test_non_iconic_inherits_from_ancestor(self, db_path, common_names_path, tmp_path):
        """Test that non-iconic taxa inherit iconic_taxon_id from nearest iconic ancestor."""
        taxonomy = generate_tree_with_iconic_taxa()

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        # NonIconicClass (id=50) is under Animalia (id=1)
        # It should inherit iconic_taxon_id=1 from Animalia
        assert results[50]['iconic_taxon_id'] == 1, 'Non-iconic class should inherit from Animalia'

        # Species under non-iconic class should also inherit from Animalia
        assert results[102]['iconic_taxon_id'] == 1, (
            'Species under non-iconic class should inherit from Animalia'
        )

    def test_no_iconic_ancestor(self, db_path, common_names_path, tmp_path):
        """Test that taxa with no iconic ancestors have iconic_taxon_id=None."""
        taxonomy = generate_tree_with_iconic_taxa()

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        # Bacteria (id=200) and its descendants have no iconic ancestors
        assert results[200]['iconic_taxon_id'] is None, 'Bacteria should have no iconic taxon'
        assert results[201]['iconic_taxon_id'] is None, (
            'Bacteria species should have no iconic taxon'
        )

    def test_root_has_no_iconic_taxon(self, db_path, common_names_path, tmp_path):
        """Test that root taxon has no iconic_taxon_id."""
        taxonomy = generate_tree_with_iconic_taxa()

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        root_id = taxonomy.root_id
        assert results[root_id]['iconic_taxon_id'] is None, 'Root should have no iconic taxon'


class TestTreeVariants:
    """Tests for various tree structures."""

    def test_single_node_tree(self, db_path, common_names_path, tmp_path):
        """Test aggregation on a tree with only root node."""
        taxonomy = generate_synthetic_taxonomy(
            depth=0,
            branching_factor=0,
            observation_count_range=(5, 5),
            seed=42,
        )

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)
        root_id = taxonomy.root_id

        assert results[root_id]['observations_count_rg'] == 5
        assert results[root_id]['leaf_taxa_count'] == 1
        assert _parse_id_list(results[root_id]['ancestor_ids']) == []
        assert _parse_id_list(results[root_id]['child_ids']) == []

    def test_linear_chain(self, db_path, common_names_path, tmp_path):
        """Test aggregation on a linear chain (each node has one child)."""
        taxonomy = generate_linear_chain(length=10, seed=42)

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        # Root should have all observations aggregated
        root_id = taxonomy.root_id
        total_obs = sum(n.observation_count for n in taxonomy.nodes.values())
        assert results[root_id]['observations_count_rg'] == total_obs

        # Root should have leaf_count of 1 (only one path to leaf)
        assert results[root_id]['leaf_taxa_count'] == 1

    def test_wide_shallow_tree(self, db_path, common_names_path, tmp_path):
        """Test aggregation on a wide, shallow tree."""
        taxonomy = generate_wide_shallow_tree(width=10, depth=2, seed=42)

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual = results[node.id]['observations_count_rg']
            expected = node.expected_obs_count
            assert actual == expected, (
                f'Taxon {node.id}: expected obs_count={expected}, got {actual}'
            )

    def test_mid_tree_leaves(self, db_path, common_names_path, tmp_path):
        """Test leaf count with mid-tree leaves (genera with no species)."""
        taxonomy = generate_tree_with_mid_leaves(
            depth=4,
            branching_factor=3,
            leaf_probability=0.3,
            seed=42,
        )

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual_leaf = results[node.id]['leaf_taxa_count']
            expected_leaf = node.expected_leaf_count
            assert actual_leaf == expected_leaf, (
                f'Taxon {node.id}: expected leaf_count={expected_leaf}, got {actual_leaf}'
            )

    def test_imbalanced_tree(self, db_path, common_names_path, tmp_path):
        """Test correct aggregation on heavily imbalanced tree (Arthropoda-like scenario)."""
        taxonomy = generate_imbalanced_tree(
            depth=5,
            branching_factor=3,
            heavy_branch_multiplier=2.0,
            seed=42,
        )

        _load_synthetic_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy.nodes.values():
            actual_obs = results[node.id]['observations_count_rg']
            expected_obs = node.expected_obs_count
            assert actual_obs == expected_obs, (
                f'Taxon {node.id}: expected obs_count={expected_obs}, got {actual_obs}'
            )

            actual_leaf = results[node.id]['leaf_taxa_count']
            expected_leaf = node.expected_leaf_count
            assert actual_leaf == expected_leaf, (
                f'Taxon {node.id}: expected leaf_count={expected_leaf}, got {actual_leaf}'
            )
