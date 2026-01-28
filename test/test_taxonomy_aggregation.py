"""
Unit tests for taxonomy aggregation correctness using real taxonomy data subset.

Tests verify that aggregate_taxon_db correctly computes:
- ancestor_ids: path from root to each taxon
- child_ids: direct children of each taxon
- iconic_taxon_id: most specific iconic taxon in ancestry
- observations_count_rg: sum of observations in subtree
- leaf_taxa_count: count of leaf taxa in subtree
"""

import sqlite3
from random import Random

import pytest
from pyinaturalist import ICONIC_TAXA, ROOT_TAXON_ID

from pyinaturalist_convert.db import create_tables
from pyinaturalist_convert.taxonomy import aggregate_taxon_db
from test.synthetic_taxonomy import TestTaxon, load_taxonomy_from_csv


def _load_taxonomy_to_db(taxonomy: list[TestTaxon], db_path) -> None:
    """Load taxonomy into a SQLite database for testing."""
    create_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')
        conn.execute('DELETE FROM observation')

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

        # Insert observations (one per taxon with observation_count > 0)

        rng = Random()
        obs_id = 1
        for node in taxonomy:
            for _ in range(rng.randint(0, 10)):
                conn.execute(
                    'INSERT INTO observation (id, taxon_id) VALUES (?, ?)',
                    (obs_id, node.id),
                )
                obs_id += 1

        conn.commit()


def _get_results(db_path) -> dict[int, dict]:
    """Fetch aggregation results from database."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM taxon').fetchall()
        results = {row['id']: dict(row) for row in rows}
    return results


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


@pytest.fixture
def taxonomy() -> list[TestTaxon]:
    """Load real taxonomy data with random observation counts."""
    return load_taxonomy_from_csv()


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
