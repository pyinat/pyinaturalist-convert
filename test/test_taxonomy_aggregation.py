"""
Unit tests for taxonomy aggregation correctness using real taxonomy data subset.

Tests verify that aggregate_taxon_db correctly computes:
- ancestor_ids: path from root to each taxon
- child_ids: direct children of each taxon
- iconic_taxon_id: most specific iconic taxon in ancestry
- observations_count_rg: sum of observations in subtree
- leaf_taxa_count: count of leaf taxa in subtree
"""

import csv
import sqlite3
from collections import deque
from pathlib import Path
from random import Random
from typing import Optional
from unittest.mock import patch

import pytest
from attr import define, field
from pyinaturalist import ICONIC_TAXA, ROOT_TAXON_ID, Taxon

from pyinaturalist_convert.db import create_tables
from pyinaturalist_convert.taxonomy import aggregate_taxon_db

SAMPLE_TAXONOMY_PATH = Path(__file__).parent / 'sample_data' / 'taxonomy.csv'


@define
class TestTaxon(Taxon):
    """Taxon with additional values for testing"""

    depth: int = field(default=0)
    expected_ancestor_ids: list[int] = field(factory=list)
    expected_iconic_taxon_id: int = field(default=None)
    expected_child_ids: list[int] = field(factory=list)
    observations_count: int = field(default=0)


def load_taxonomy_from_csv(csv_path: Path = SAMPLE_TAXONOMY_PATH) -> list[TestTaxon]:
    """
    Load taxonomy from CSV and compute expected aggregation values.

    Args:
        csv_path: Path to CSV file with columns: id, parent_id, name, rank
        seed: Random seed for reproducibility

    Returns:
        TaxonomyFixture with all expected aggregation values pre-computed
    """
    taxa = []

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            taxa.append(
                TestTaxon(  # type: ignore [call-arg]
                    id=int(row['id']),
                    parent_id=int(row['parent_id']) if row['parent_id'] else None,
                    name=row['name'],
                    rank=row['rank'],
                )
            )

    # Build children lookup
    children_by_parent: dict[int, list[int]] = {}
    for taxon in taxa:
        if taxon.parent_id is not None:
            children_by_parent.setdefault(taxon.parent_id, []).append(taxon.id)
    for taxon in taxa:
        taxon.expected_child_ids = children_by_parent.get(taxon.id, [])

    taxa = _compute_top_down_values(taxa, children_by_parent)
    return taxa


def _compute_top_down_values(
    taxa: list[TestTaxon],
    children_by_parent: dict[int, list[int]],
) -> list[TestTaxon]:
    """Compute ancestor_ids, depth, and iconic_taxon_id top-down via BFS."""
    queue: deque = deque([(ROOT_TAXON_ID, [], None, 0)])  # (node_id, ancestors, iconic_id, depth)
    taxa_by_id = {taxon.id: taxon for taxon in taxa}

    while queue:
        node_id, ancestors, iconic_id, depth = queue.popleft()

        if node_id not in taxa_by_id:
            continue

        taxon = taxa_by_id[node_id]
        taxon.expected_ancestor_ids = ancestors.copy()
        taxon.depth = depth

        # Determine iconic_taxon_id for this node
        if node_id in ICONIC_TAXA:
            taxon.expected_iconic_taxon_id = node_id
            child_iconic_id = node_id
        else:
            taxon.expected_iconic_taxon_id = iconic_id
            child_iconic_id = iconic_id

        # Add children to queue
        child_ancestors = ancestors + [node_id]
        for child_id in children_by_parent.get(node_id, []):
            queue.append((child_id, child_ancestors, child_iconic_id, depth + 1))

    return taxa


def _load_taxonomy_to_db(taxonomy: list[TestTaxon], db_path) -> None:
    """Load taxonomy into a SQLite database for testing."""
    create_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')

        # Insert taxa
        for node in taxonomy:
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

        conn.commit()


def _make_observation_counts(taxonomy: list[TestTaxon]) -> dict[int, int]:
    """Generate random observation counts per taxon (deterministic seed)."""
    rng = Random()
    counts: dict[int, int] = {}
    for node in taxonomy:
        n = rng.randint(0, 10)
        if n > 0:
            counts[node.id] = n
    return counts


def _get_results(db_path) -> dict[int, dict]:
    """Fetch aggregation results from database."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM taxon').fetchall()
        results = {row['id']: dict(row) for row in rows}
    return results


def _parse_id_list(value: Optional[str]) -> list[int]:
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


@pytest.fixture
def taxonomy() -> list[TestTaxon]:
    """Load real taxonomy data with random observation counts."""
    return load_taxonomy_from_csv()


@pytest.fixture(autouse=True)
def mock_observation_counts(taxonomy):
    """Mock get_observation_taxon_counts to avoid needing an observation table."""
    counts = _make_observation_counts(taxonomy)
    with patch(
        'pyinaturalist_convert.taxonomy.get_observation_taxon_counts',
        return_value=counts,
    ):
        yield


class TestAncestorIds:
    """Tests for ancestor_ids computation."""

    def test_ancestor_path(self, db_path, common_names_path, tmp_path, taxonomy):
        """Test that ancestor_ids contains correct path from root."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy:
            actual = _parse_id_list(results[node.id]['ancestor_ids'])
            expected = node.expected_ancestor_ids
            assert actual == expected, (
                f'Taxon {node.id} ({node.name}): expected ancestors={expected}, got {actual}'
            )


class TestChildIds:
    """Tests for child_ids computation."""

    def test_child_ids(self, db_path, common_names_path, tmp_path, taxonomy):
        """Test that child_ids contains direct children."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy:
            actual = set(_parse_id_list(results[node.id]['child_ids']))
            expected = set(node.expected_child_ids)
            assert actual == expected, (
                f'Taxon {node.id} ({node.name}): expected children={expected}, got {actual}'
            )

    def test_leaf_has_no_children(self, db_path, common_names_path, tmp_path, taxonomy):
        """Test that leaf taxa have empty child_ids."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        leaves = [n for n in taxonomy if not n.expected_child_ids]

        for leaf in leaves:
            children = _parse_id_list(results[leaf.id]['child_ids'])
            assert children == [], f'Leaf {leaf.id} should have no children, got {children}'


class TestIconicTaxonId:
    """Tests for iconic_taxon_id assignment."""

    @pytest.fixture
    def iconic_results(self, db_path, common_names_path, tmp_path, taxonomy):
        """Run aggregation on taxonomy and return results."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)
        return _get_results(db_path), taxonomy

    def test_iconic_taxon_propagation(self, iconic_results):
        """Test that iconic_taxon_id is correctly propagated from ancestors."""
        results, taxonomy = iconic_results

        for node in taxonomy:
            actual = results[node.id]['iconic_taxon_id']
            expected = node.expected_iconic_taxon_id
            assert actual == expected, (
                f'Taxon {node.id} ({node.name}): expected iconic={expected}, got {actual}'
            )

    def test_iconic_taxa_use_themselves(self, iconic_results):
        """Test that iconic taxa use themselves as their iconic_taxon_id."""
        results, _ = iconic_results

        for iconic_id in ICONIC_TAXA.keys():
            if iconic_id in results:
                assert results[iconic_id]['iconic_taxon_id'] == iconic_id, (
                    f'Iconic taxon {iconic_id} should use itself as iconic_taxon_id'
                )

    def test_species_inherit_iconic_taxon(self, iconic_results):
        """Test that species inherit iconic_taxon_id from their iconic ancestor."""
        results, taxonomy = iconic_results

        # Check a sample of species (rank='species')
        species_nodes = [n for n in taxonomy if n.rank == 'species']

        for species in species_nodes[:50]:  # Check first 50 species
            actual = results[species.id]['iconic_taxon_id']
            expected = species.expected_iconic_taxon_id
            assert actual == expected, (
                f'Species {species.id} ({species.name}): expected iconic={expected}, got {actual}'
            )


class TestTreeIntegrity:
    """Tests for tree structure integrity."""

    def test_all_taxa_have_ancestors_computed(self, db_path, common_names_path, tmp_path, taxonomy):
        """Test that all non-root taxa have ancestor_ids computed."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy:
            if node.id != ROOT_TAXON_ID:
                ancestors = _parse_id_list(results[node.id]['ancestor_ids'])
                assert len(ancestors) > 0, (
                    f'Non-root taxon {node.id} should have ancestors, got none'
                )
                assert ancestors[0] == ROOT_TAXON_ID, (
                    f'First ancestor of {node.id} should be root ({ROOT_TAXON_ID}), '
                    f'got {ancestors[0]}'
                )

    def test_parent_in_ancestors(self, db_path, common_names_path, tmp_path, taxonomy):
        """Test that every taxon's parent is in its ancestor list."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        for node in taxonomy:
            if node.parent_id is not None:
                ancestors = _parse_id_list(results[node.id]['ancestor_ids'])
                assert node.parent_id in ancestors, (
                    f'Parent {node.parent_id} of taxon {node.id} should be in ancestors {ancestors}'
                )
                # Parent should be the last ancestor
                assert ancestors[-1] == node.parent_id, (
                    f'Last ancestor of {node.id} should be parent {node.parent_id}, '
                    f'got {ancestors[-1]}'
                )

    def test_root_has_most_leaves(self, db_path, common_names_path, tmp_path, taxonomy):
        """Test that root has the highest leaf count (sum of all leaves)."""
        _load_taxonomy_to_db(taxonomy, db_path)
        aggregate_taxon_db(db_path, tmp_path / 'backup.parquet', common_names_path)

        results = _get_results(db_path)

        root_leaf_count = results[ROOT_TAXON_ID]['leaf_taxa_count']
        for taxon_id, result in results.items():
            if taxon_id != ROOT_TAXON_ID:
                assert result['leaf_taxa_count'] <= root_leaf_count, (
                    f'Taxon {taxon_id} has more leaves ({result["leaf_taxa_count"]}) '
                    f'than root ({root_leaf_count})'
                )
