#!/usr/bin/env python3
"""Export a representative subset of inat taxonomy to use as test data"""

import csv
import sqlite3
from pathlib import Path

from pyinaturalist import ICONIC_TAXA as BASE_ICONIC_TAXA
from pyinaturalist import ROOT_TAXON_ID

from pyinaturalist_convert import DB_PATH

OUTPUT_PATH = Path(__file__).parent.parent / 'test' / 'sample_data' / 'taxonomy.csv'
ICONIC_TAXA = BASE_ICONIC_TAXA.copy()
ICONIC_TAXA.pop(0, None)
LEAVES_PER_GROUP = 50


def get_leaf_sample(conn: sqlite3.Connection, iconic_taxon_id: int, limit: int) -> list[int]:
    """Sample leaf taxa (species/subspecies) that descend from an iconic taxon"""
    cursor = conn.execute(
        """
        WITH RECURSIVE descendants AS (
            -- Base case: start from the iconic taxon
            SELECT id, rank FROM taxon WHERE id = ?

            UNION ALL

            -- Recursive case: find children
            SELECT t.id, t.rank
            FROM taxon t
            JOIN descendants d ON t.parent_id = d.id
        )
        SELECT id FROM descendants
        WHERE rank IN ('species', 'subspecies')
        ORDER BY id ASC
        LIMIT ?
        """,
        (iconic_taxon_id, limit),
    )
    return [row[0] for row in cursor.fetchall()]


def get_ancestors(conn: sqlite3.Connection, taxon_ids: set[int]) -> set[int]:
    """Get all ancestor IDs for a set of taxa"""
    if not taxon_ids:
        return set()

    placeholders = ','.join('?' * len(taxon_ids))
    cursor = conn.execute(
        f"""
        WITH RECURSIVE ancestors AS (
            -- Base case: start from the given taxa
            SELECT id, parent_id FROM taxon WHERE id IN ({placeholders})

            UNION

            -- Recursive case: find parents
            SELECT t.id, t.parent_id
            FROM taxon t
            JOIN ancestors a ON t.id = a.parent_id
            WHERE a.parent_id IS NOT NULL
        )
        SELECT DISTINCT id FROM ancestors
        """,
        list(taxon_ids),
    )

    return {row[0] for row in cursor.fetchall()}


def export_taxa(conn: sqlite3.Connection, taxon_ids: set[int], output_path: Path) -> None:
    """Export selected taxa to CSV"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    placeholders = ','.join('?' * len(taxon_ids))
    cursor = conn.execute(
        f"""
        SELECT
            id,
            parent_id,
            name,
            rank
        FROM taxon
        WHERE id IN ({placeholders})
        ORDER BY id
        """,
        list(taxon_ids),
    )

    rows = cursor.fetchall()
    columns = [
        'id',
        'parent_id',
        'name',
        'rank',
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f'Exported {len(rows)} taxa to {output_path}')


def main():
    conn = sqlite3.connect(DB_PATH)
    selected_taxa: set[int] = set()

    # Always include root + iconic taxa
    selected_taxa.add(ROOT_TAXON_ID)
    selected_taxa.update(ICONIC_TAXA.keys())

    # Sample leaves from each iconic taxon group
    for iconic_id, name in ICONIC_TAXA.items():
        leaf_taxa = get_leaf_sample(conn, iconic_id, LEAVES_PER_GROUP)
        selected_taxa.update(leaf_taxa)
        print(f'Sampled {len(leaf_taxa)} leaves from {name} (iconic_id={iconic_id})')
    print(f'\nTotal leaf/iconic taxa selected: {len(selected_taxa)}')

    all_taxa = get_ancestors(conn, selected_taxa)
    print(f'Total with ancestors: {len(all_taxa)} taxa')

    export_taxa(conn, all_taxa, OUTPUT_PATH)
    conn.close()


if __name__ == '__main__':
    main()
