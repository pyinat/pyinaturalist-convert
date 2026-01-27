"""
Synthetic taxonomy dataset generator for testing taxonomy aggregation.

Generates tree structures with configurable properties:
- Depth and branching factor
- Imbalanced branches (simulating Arthropoda-like heavy subtrees)
- Mid-tree leaves (genera/families with no children)
- Observation counts for aggregation testing
"""

from dataclasses import dataclass, field
from pathlib import Path
from random import Random
from typing import Dict, List, Optional, Tuple

# Standard taxonomic ranks in order from root to leaf
RANKS = [
    'root',
    'kingdom',
    'phylum',
    'class',
    'order',
    'family',
    'genus',
    'species',
    'subspecies',
]

# Iconic taxa IDs matching pyinaturalist.constants.ICONIC_TAXA
# These are real iNaturalist taxon IDs for major groups
ICONIC_TAXA_IDS = {
    1: 'Animalia',
    3: 'Aves',
    20978: 'Amphibia',
    26036: 'Reptilia',
    40151: 'Mammalia',
    47115: 'Mollusca',
    47119: 'Arachnida',
    47126: 'Plantae',
    47158: 'Insecta',
    47170: 'Fungi',
    47178: 'Actinopterygii',
    48222: 'Chromista',
}


@dataclass
class TaxonNode:
    """A single taxon in the synthetic tree."""

    id: int
    name: str
    parent_id: Optional[int]
    rank: str
    observation_count: int = 0
    is_iconic: bool = False

    # Computed during tree building (for test validation)
    expected_obs_count: int = 0
    expected_leaf_count: int = 0
    expected_ancestor_ids: List[int] = field(default_factory=list)
    expected_child_ids: List[int] = field(default_factory=list)
    expected_iconic_taxon_id: Optional[int] = None
    depth: int = 0


@dataclass
class SyntheticTaxonomy:
    """A complete synthetic taxonomy tree with expected aggregation values."""

    nodes: Dict[int, TaxonNode]
    root_id: int
    max_depth: int

    def to_csv_rows(self) -> List[Dict]:
        """Convert to list of dicts for CSV export."""
        rows = []
        for node in self.nodes.values():
            rows.append(
                {
                    'id': node.id,
                    'name': node.name,
                    'parent_id': node.parent_id if node.parent_id is not None else '',
                    'rank': node.rank,
                    'observations_count_rg': node.observation_count,
                    'expected_obs_count': node.expected_obs_count,
                    'expected_leaf_count': node.expected_leaf_count,
                    'expected_ancestor_ids': ','.join(map(str, node.expected_ancestor_ids)),
                    'expected_child_ids': ','.join(map(str, node.expected_child_ids)),
                    'expected_iconic_taxon_id': node.expected_iconic_taxon_id or '',
                    'depth': node.depth,
                }
            )
        return rows

    def to_csv(self, path: Path) -> None:
        """Write taxonomy to CSV file."""
        import csv

        rows = self.to_csv_rows()
        if not rows:
            return

        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def to_dataframe(self) -> 'DataFrame':
        """Convert to pandas DataFrame."""
        import pandas as pd

        return pd.DataFrame(self.to_csv_rows())


def generate_synthetic_taxonomy(
    depth: int = 5,
    branching_factor: int = 3,
    imbalance_factor: float = 1.0,
    mid_tree_leaf_probability: float = 0.0,
    observation_count_range: Tuple[int, int] = (0, 10),
    iconic_taxon_ids: Optional[List[int]] = None,
    seed: int = 42,
) -> SyntheticTaxonomy:
    """
    Generate a synthetic taxonomy tree.

    Args:
        depth: Maximum depth of the tree (0 = root only)
        branching_factor: Base number of children per node
        imbalance_factor: Multiplier for first child's subtree size (>1 creates heavy branch)
        mid_tree_leaf_probability: Probability that a non-leaf node has no children
        observation_count_range: (min, max) range for random observation counts
        iconic_taxon_ids: List of taxon IDs to mark as iconic (for iconic_taxon_id testing)
        seed: Random seed for reproducibility

    Returns:
        SyntheticTaxonomy with all expected aggregation values pre-computed
    """
    rng = Random(seed)
    nodes: Dict[int, TaxonNode] = {}
    next_id = 48460  # Match iNaturalist root ID convention

    iconic_set = set(iconic_taxon_ids) if iconic_taxon_ids else set()

    def get_rank(current_depth: int) -> str:
        """Get taxonomic rank for given depth."""
        if current_depth < len(RANKS):
            return RANKS[current_depth]
        return 'subspecies'  # Default for depths beyond rank list

    def generate_subtree(
        parent_id: Optional[int],
        current_depth: int,
        ancestor_ids: List[int],
        iconic_ancestor_id: Optional[int],
    ) -> List[int]:
        """Recursively generate a subtree, returning list of created node IDs."""
        nonlocal next_id

        if current_depth > depth:
            return []

        # Determine number of children (apply imbalance to first child)
        if current_depth == depth:
            num_children = 0  # Leaf level
        elif rng.random() < mid_tree_leaf_probability and current_depth > 0:
            num_children = 0  # Random mid-tree leaf
        else:
            num_children = branching_factor

        created_ids = []

        for child_idx in range(max(1, num_children) if current_depth == 0 else num_children or 1):
            node_id = next_id
            next_id += 1

            # Create node
            is_iconic = node_id in iconic_set
            obs_count = rng.randint(*observation_count_range)

            node = TaxonNode(
                id=node_id,
                name=f'taxon_{node_id}',
                parent_id=parent_id,
                rank=get_rank(current_depth),
                observation_count=obs_count,
                is_iconic=is_iconic,
                depth=current_depth,
                expected_ancestor_ids=ancestor_ids.copy(),
                expected_iconic_taxon_id=iconic_ancestor_id if not is_iconic else node_id,
            )

            # Update iconic ancestor for children
            child_iconic_id = node_id if is_iconic else iconic_ancestor_id

            nodes[node_id] = node
            created_ids.append(node_id)

            # Generate children (apply imbalance factor to first child)
            if child_idx == 0 and imbalance_factor > 1.0:
                # First child gets deeper subtree
                min(depth + int(imbalance_factor), len(RANKS) - 1)

            # Determine if this node should be a mid-tree leaf
            is_mid_tree_leaf = current_depth < depth and current_depth > 0 and num_children == 0

            if not is_mid_tree_leaf and current_depth < depth:
                child_ids = generate_subtree(
                    parent_id=node_id,
                    current_depth=current_depth + 1,
                    ancestor_ids=ancestor_ids + [node_id],
                    iconic_ancestor_id=child_iconic_id,
                )
                node.expected_child_ids = child_ids

            # Handle root node specially (only one root)
            if current_depth == 0:
                break

        return created_ids

    # Generate tree starting from root
    generate_subtree(
        parent_id=None,
        current_depth=0,
        ancestor_ids=[],
        iconic_ancestor_id=None,
    )

    # Compute expected aggregation values bottom-up
    _compute_expected_aggregates(nodes)

    root_id = min(nodes.keys())  # Root is first created node

    return SyntheticTaxonomy(
        nodes=nodes,
        root_id=root_id,
        max_depth=depth,
    )


def _compute_expected_aggregates(nodes: Dict[int, TaxonNode]) -> None:
    """Compute expected observation counts and leaf counts bottom-up."""
    # Build children lookup
    children_by_parent: Dict[int, List[int]] = {}
    for node in nodes.values():
        if node.parent_id is not None:
            children_by_parent.setdefault(node.parent_id, []).append(node.id)

    # Sort nodes by depth descending (process leaves first)
    sorted_nodes = sorted(nodes.values(), key=lambda n: n.depth, reverse=True)

    for node in sorted_nodes:
        child_ids = children_by_parent.get(node.id, [])

        if not child_ids:
            # Leaf node
            node.expected_leaf_count = 1
            node.expected_obs_count = node.observation_count
        else:
            # Internal node: sum children
            node.expected_leaf_count = sum(nodes[cid].expected_leaf_count for cid in child_ids)
            node.expected_obs_count = node.observation_count + sum(
                nodes[cid].expected_obs_count for cid in child_ids
            )


def generate_linear_chain(length: int = 5, seed: int = 42) -> SyntheticTaxonomy:
    """Generate a simple linear chain (each node has exactly one child)."""
    return generate_synthetic_taxonomy(
        depth=length - 1,
        branching_factor=1,
        seed=seed,
    )


def generate_wide_shallow_tree(
    width: int = 10, depth: int = 2, seed: int = 42
) -> SyntheticTaxonomy:
    """Generate a wide, shallow tree."""
    return generate_synthetic_taxonomy(
        depth=depth,
        branching_factor=width,
        seed=seed,
    )


def generate_imbalanced_tree(
    depth: int = 6,
    branching_factor: int = 3,
    heavy_branch_multiplier: float = 2.0,
    seed: int = 42,
) -> SyntheticTaxonomy:
    """Generate a tree with one heavy branch (like Arthropoda)."""
    return generate_synthetic_taxonomy(
        depth=depth,
        branching_factor=branching_factor,
        imbalance_factor=heavy_branch_multiplier,
        seed=seed,
    )


def generate_tree_with_mid_leaves(
    depth: int = 5,
    branching_factor: int = 3,
    leaf_probability: float = 0.2,
    seed: int = 42,
) -> SyntheticTaxonomy:
    """Generate a tree with random mid-tree leaves (like genera with no species)."""
    return generate_synthetic_taxonomy(
        depth=depth,
        branching_factor=branching_factor,
        mid_tree_leaf_probability=leaf_probability,
        seed=seed,
    )


def generate_tree_with_iconic_taxa(seed: int = 42) -> SyntheticTaxonomy:
    """
    Generate a tree with specific iconic taxa for testing iconic_taxon_id propagation.

    Creates a structure matching iNaturalist's expected hierarchy:
        root (48460)
        ├── Animalia (1) [iconic] - kingdom
        │   └── Chordata (2) - phylum (required for current implementation)
        │       ├── Aves (3) [iconic] - class
        │       │   └── bird_species_1 (100)
        │       │   └── bird_species_2 (101)
        │       └── NonIconicClass (50) - class
        │           └── animal_species (102)
        └── Plantae (47126) [iconic] - kingdom
            └── Tracheophyta (500) - phylum
                └── plant_species (103)
        └── Bacteria (200) - kingdom (no iconic)
            └── BacteriaPhylum (202) - phylum
                └── bacteria_species (201)

    Expected iconic_taxon_id:
        - root: None
        - Animalia: 1 (itself)
        - Chordata: 1 (from Animalia)
        - Aves: 3 (itself, overrides parent)
        - species under Aves: 3
        - NonIconicClass: 1 (from Animalia)
        - species under NonIconicClass: 1
        - Plantae: 47126 (itself)
        - Tracheophyta: 47126 (from Plantae)
        - species under Plantae: 47126
        - Bacteria + descendants: None
    """
    from pyinaturalist.constants import ROOT_TAXON_ID

    nodes: Dict[int, TaxonNode] = {}

    # Root
    nodes[ROOT_TAXON_ID] = TaxonNode(
        id=ROOT_TAXON_ID,
        name='Life',
        parent_id=None,
        rank='root',
        observation_count=1,
        depth=0,
        expected_ancestor_ids=[],
        expected_iconic_taxon_id=None,
    )

    # Animalia (iconic kingdom)
    nodes[1] = TaxonNode(
        id=1,
        name='Animalia',
        parent_id=ROOT_TAXON_ID,
        rank='kingdom',
        observation_count=1,
        is_iconic=True,
        depth=1,
        expected_ancestor_ids=[ROOT_TAXON_ID],
        expected_iconic_taxon_id=1,
    )

    # Chordata (phylum under Animalia - required for current impl's phylum partitioning)
    nodes[2] = TaxonNode(
        id=2,
        name='Chordata',
        parent_id=1,
        rank='phylum',
        observation_count=1,
        is_iconic=False,
        depth=2,
        expected_ancestor_ids=[ROOT_TAXON_ID, 1],
        expected_iconic_taxon_id=1,  # Inherits from Animalia
    )

    # Aves (iconic class under Chordata)
    nodes[3] = TaxonNode(
        id=3,
        name='Aves',
        parent_id=2,
        rank='class',
        observation_count=1,
        is_iconic=True,
        depth=3,
        expected_ancestor_ids=[ROOT_TAXON_ID, 1, 2],
        expected_iconic_taxon_id=3,  # Itself (overrides Animalia)
    )

    # Bird species under Aves
    for i, sp_id in enumerate([100, 101]):
        nodes[sp_id] = TaxonNode(
            id=sp_id,
            name=f'bird_species_{i}',
            parent_id=3,
            rank='species',
            observation_count=1,
            depth=4,
            expected_ancestor_ids=[ROOT_TAXON_ID, 1, 2, 3],
            expected_iconic_taxon_id=3,  # From Aves
        )

    # NonIconicClass under Chordata
    nodes[50] = TaxonNode(
        id=50,
        name='NonIconicClass',
        parent_id=2,
        rank='class',
        observation_count=1,
        is_iconic=False,
        depth=3,
        expected_ancestor_ids=[ROOT_TAXON_ID, 1, 2],
        expected_iconic_taxon_id=1,  # Inherits from Animalia
    )

    # Species under NonIconicClass
    nodes[102] = TaxonNode(
        id=102,
        name='animal_species_0',
        parent_id=50,
        rank='species',
        observation_count=1,
        depth=4,
        expected_ancestor_ids=[ROOT_TAXON_ID, 1, 2, 50],
        expected_iconic_taxon_id=1,  # Inherits from Animalia
    )

    # Plantae (iconic kingdom)
    nodes[47126] = TaxonNode(
        id=47126,
        name='Plantae',
        parent_id=ROOT_TAXON_ID,
        rank='kingdom',
        observation_count=1,
        is_iconic=True,
        depth=1,
        expected_ancestor_ids=[ROOT_TAXON_ID],
        expected_iconic_taxon_id=47126,
    )

    # Tracheophyta (phylum under Plantae)
    nodes[500] = TaxonNode(
        id=500,
        name='Tracheophyta',
        parent_id=47126,
        rank='phylum',
        observation_count=1,
        is_iconic=False,
        depth=2,
        expected_ancestor_ids=[ROOT_TAXON_ID, 47126],
        expected_iconic_taxon_id=47126,  # From Plantae
    )

    # Plant species
    nodes[103] = TaxonNode(
        id=103,
        name='plant_species_0',
        parent_id=500,
        rank='species',
        observation_count=1,
        depth=3,
        expected_ancestor_ids=[ROOT_TAXON_ID, 47126, 500],
        expected_iconic_taxon_id=47126,  # From Plantae
    )

    # Bacteria (non-iconic kingdom)
    nodes[200] = TaxonNode(
        id=200,
        name='Bacteria',
        parent_id=ROOT_TAXON_ID,
        rank='kingdom',
        observation_count=1,
        is_iconic=False,
        depth=1,
        expected_ancestor_ids=[ROOT_TAXON_ID],
        expected_iconic_taxon_id=None,
    )

    # BacteriaPhylum (phylum under Bacteria)
    nodes[202] = TaxonNode(
        id=202,
        name='BacteriaPhylum',
        parent_id=200,
        rank='phylum',
        observation_count=1,
        is_iconic=False,
        depth=2,
        expected_ancestor_ids=[ROOT_TAXON_ID, 200],
        expected_iconic_taxon_id=None,
    )

    # Bacteria species
    nodes[201] = TaxonNode(
        id=201,
        name='bacteria_species',
        parent_id=202,
        rank='species',
        observation_count=1,
        depth=3,
        expected_ancestor_ids=[ROOT_TAXON_ID, 200, 202],
        expected_iconic_taxon_id=None,
    )

    # Set up child IDs
    for node in nodes.values():
        node.expected_child_ids = [n.id for n in nodes.values() if n.parent_id == node.id]

    # Compute expected aggregates
    _compute_expected_aggregates(nodes)

    return SyntheticTaxonomy(
        nodes=nodes,
        root_id=ROOT_TAXON_ID,
        max_depth=4,
    )
