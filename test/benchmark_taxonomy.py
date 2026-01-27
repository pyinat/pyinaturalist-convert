#!/usr/bin/env python
"""
Benchmark harness for taxonomy aggregation performance.

Usage:
    python -m test.benchmark_taxonomy [--sizes 100,1000,10000] [--implementations old,new]

Measures:
    - Wall-clock time for full aggregation
    - Peak memory usage
    - Time per taxon

Outputs results as a table for comparison.
"""

import argparse
import gc
import sqlite3
import sys
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Callable, List, Optional
from unittest.mock import patch

from pyinaturalist_convert.db import create_tables
from pyinaturalist_convert.taxonomy import aggregate_taxon_db, aggregate_taxon_db_v2
from test.synthetic_taxonomy import (
    SyntheticTaxonomy,
    generate_synthetic_taxonomy,
)


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    implementation: str
    num_taxa: int
    wall_time_seconds: float
    peak_memory_mb: float
    time_per_taxon_us: float

    def __str__(self) -> str:
        return (
            f'{self.implementation:12} | {self.num_taxa:>8} taxa | '
            f'{self.wall_time_seconds:>8.3f}s | '
            f'{self.peak_memory_mb:>8.1f} MB | '
            f'{self.time_per_taxon_us:>8.1f} µs/taxon'
        )


def load_taxonomy_to_db(taxonomy: SyntheticTaxonomy, db_path: Path) -> None:
    """Load synthetic taxonomy into SQLite database."""
    create_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')
        conn.execute('DELETE FROM observation')

        # Insert taxa
        for node in taxonomy.nodes.values():
            conn.execute(
                """INSERT INTO taxon (id, parent_id, name, rank, observations_count_rg, leaf_taxa_count)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (node.id, node.parent_id, node.name, node.rank, 0, 0),
            )

        # Insert observations
        obs_id = 1
        for node in taxonomy.nodes.values():
            for _ in range(node.observation_count):
                conn.execute(
                    'INSERT INTO observation (id, taxon_id) VALUES (?, ?)',
                    (obs_id, node.id),
                )
                obs_id += 1

        conn.commit()


def generate_taxonomy_for_size(target_size: int, seed: int = 42) -> SyntheticTaxonomy:
    """Generate a taxonomy with approximately the target number of taxa."""
    # Estimate branching factor needed for target size
    # For a balanced tree: size ≈ (b^(d+1) - 1) / (b - 1)
    # Use depth=6 and solve for branching factor
    depth = 6

    # Binary search for branching factor
    low, high = 2, 20
    best_taxonomy = None
    best_diff = float('inf')

    while low <= high:
        mid = (low + high) // 2
        taxonomy = generate_synthetic_taxonomy(
            depth=depth,
            branching_factor=mid,
            observation_count_range=(0, 5),
            seed=seed,
        )
        size = len(taxonomy.nodes)
        diff = abs(size - target_size)

        if diff < best_diff:
            best_diff = diff
            best_taxonomy = taxonomy

        if size < target_size:
            low = mid + 1
        elif size > target_size:
            high = mid - 1
        else:
            break

    return best_taxonomy


def run_benchmark(
    taxonomy: SyntheticTaxonomy,
    implementation: str = 'current',
    aggregate_func: Optional[Callable] = None,
) -> BenchmarkResult:
    """Run a single benchmark iteration."""
    if aggregate_func is None:
        aggregate_func = aggregate_taxon_db

    num_taxa = len(taxonomy.nodes)
    is_v2 = implementation in ('new', 'v2')

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        db_path = tmp_path / 'benchmark.db'
        backup_path = tmp_path / 'backup.parquet'
        common_names_path = tmp_path / 'common_names.csv'
        common_names_path.write_text('id,vernacularName\n')

        # Load data
        load_taxonomy_to_db(taxonomy, db_path)

        # Force garbage collection before measuring
        gc.collect()

        # Start memory tracking
        tracemalloc.start()

        # Time the aggregation
        start_time = perf_counter()

        if is_v2:
            # v2 doesn't use ProcessPoolExecutor
            aggregate_func(db_path, backup_path, common_names_path)
        else:
            # Patch ProcessPoolExecutor to ThreadPoolExecutor for more consistent timing
            # (avoids process startup overhead in benchmarks)
            with (
                patch('pyinaturalist_convert.taxonomy.ProcessPoolExecutor', ThreadPoolExecutor),
                patch('pyinaturalist_convert.taxonomy.sleep'),
            ):
                aggregate_func(db_path, backup_path, common_names_path, progress_bars=False)

        end_time = perf_counter()

        # Get peak memory
        _, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        wall_time = end_time - start_time
        peak_memory_mb = peak_memory / (1024 * 1024)
        time_per_taxon_us = (wall_time / num_taxa) * 1_000_000

        return BenchmarkResult(
            implementation=implementation,
            num_taxa=num_taxa,
            wall_time_seconds=wall_time,
            peak_memory_mb=peak_memory_mb,
            time_per_taxon_us=time_per_taxon_us,
        )


def run_benchmarks(
    sizes: List[int],
    implementations: List[str],
    num_iterations: int = 3,
) -> List[BenchmarkResult]:
    """Run benchmarks for all sizes and implementations."""
    results = []

    for size in sizes:
        print(f'\nGenerating taxonomy with ~{size} taxa...', file=sys.stderr)
        taxonomy = generate_taxonomy_for_size(size)
        actual_size = len(taxonomy.nodes)
        print(f'  Actual size: {actual_size} taxa', file=sys.stderr)

        for impl in implementations:
            print(f'  Benchmarking {impl}...', file=sys.stderr)

            # Get the appropriate function
            if impl == 'current' or impl == 'v1':
                func = aggregate_taxon_db
            elif impl == 'new' or impl == 'v2':
                func = aggregate_taxon_db_v2
            else:
                print(f'    Unknown implementation: {impl}', file=sys.stderr)
                continue

            # Run multiple iterations and take the best (least affected by noise)
            best_result = None
            for i in range(num_iterations):
                result = run_benchmark(taxonomy, impl, func)
                if best_result is None or result.wall_time_seconds < best_result.wall_time_seconds:
                    best_result = result
                print(f'    Iteration {i + 1}: {result.wall_time_seconds:.3f}s', file=sys.stderr)

            if best_result:
                results.append(best_result)

    return results


def print_results(results: List[BenchmarkResult]) -> None:
    """Print benchmark results as a formatted table."""
    print('\n' + '=' * 80)
    print('BENCHMARK RESULTS')
    print('=' * 80)
    print(
        f'{"Implementation":12} | {"Size":>12} | {"Time":>10} | {"Memory":>10} | {"Per Taxon":>14}'
    )
    print('-' * 80)

    for result in results:
        print(result)

    print('=' * 80)


def main():
    parser = argparse.ArgumentParser(description='Benchmark taxonomy aggregation')
    parser.add_argument(
        '--sizes',
        type=str,
        default='100,500,1000,5000',
        help='Comma-separated list of target taxonomy sizes',
    )
    parser.add_argument(
        '--implementations',
        type=str,
        default='current',
        help='Comma-separated list of implementations to benchmark (current, new)',
    )
    parser.add_argument(
        '--iterations',
        type=int,
        default=3,
        help='Number of iterations per benchmark (takes best result)',
    )

    args = parser.parse_args()

    sizes = [int(s.strip()) for s in args.sizes.split(',')]
    implementations = [s.strip() for s in args.implementations.split(',')]

    print(f'Benchmarking sizes: {sizes}')
    print(f'Implementations: {implementations}')
    print(f'Iterations per benchmark: {args.iterations}')

    results = run_benchmarks(sizes, implementations, args.iterations)
    print_results(results)


if __name__ == '__main__':
    main()
