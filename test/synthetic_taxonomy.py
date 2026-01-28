"""
Real taxonomy data loader for testing taxonomy aggregation.

Loads a subset of real iNaturalist taxonomy data from CSV and computes
expected aggregation values for test verification.
"""

import csv
from collections import deque
from pathlib import Path

from attr import define, field
from pyinaturalist import ICONIC_TAXA, ROOT_TAXON_ID, Taxon

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
