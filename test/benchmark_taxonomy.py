#!/usr/bin/env python
"""
Benchmark harness for taxonomy aggregation performance.

Usage:
    python -m test.benchmark_taxonomy [--iterations 5]

Measures:
    - Wall-clock time for full aggregation
    - Peak memory usage
    - Time per taxon

Uses the real taxonomy sample data (~1683 taxa) for realistic benchmarking.
"""

import argparse
import gc
import sqlite3
import sys
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyinaturalist_convert.db import create_tables
from pyinaturalist_convert.taxonomy import aggregate_taxon_db
from test.synthetic_taxonomy import TestTaxon, load_taxonomy_from_csv


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    num_taxa: int
    wall_time_seconds: float
    peak_memory_mb: float
    time_per_taxon_us: float

    def __str__(self) -> str:
        return (
            f'{self.num_taxa:>8} taxa | '
            f'{self.wall_time_seconds:>8.3f}s | '
            f'{self.peak_memory_mb:>8.1f} MB | '
            f'{self.time_per_taxon_us:>8.1f} µs/taxon'
        )


def load_taxonomy_to_db(taxonomy: list[TestTaxon], db_path: Path) -> None:
    """Load taxonomy into SQLite database."""
    create_tables(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM taxon')
        conn.execute('DELETE FROM observation')

        for node in taxonomy:
            conn.execute(
                """INSERT INTO taxon (id, parent_id, name, rank, observations_count_rg, leaf_taxa_count)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (node.id, node.parent_id, node.name, node.rank, 0, 0),
            )

        obs_id = 1
        for node in taxonomy:
            for _ in range(2):
                conn.execute(
                    'INSERT INTO observation (id, taxon_id) VALUES (?, ?)',
                    (obs_id, node.id),
                )
                obs_id += 1

        conn.commit()


def run_benchmark(taxonomy: list[TestTaxon]) -> BenchmarkResult:
    """Run a single benchmark iteration."""
    num_taxa = len(taxonomy)

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        db_path = tmp_path / 'benchmark.db'
        backup_path = tmp_path / 'backup.parquet'
        common_names_path = tmp_path / 'common_names.csv'
        common_names_path.write_text('id,vernacularName\n')

        load_taxonomy_to_db(taxonomy, db_path)

        gc.collect()
        tracemalloc.start()

        start_time = perf_counter()
        aggregate_taxon_db(db_path, backup_path, common_names_path)
        end_time = perf_counter()

        _, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        wall_time = end_time - start_time
        peak_memory_mb = peak_memory / (1024 * 1024)
        time_per_taxon_us = (wall_time / num_taxa) * 1_000_000

        return BenchmarkResult(
            num_taxa=num_taxa,
            wall_time_seconds=wall_time,
            peak_memory_mb=peak_memory_mb,
            time_per_taxon_us=time_per_taxon_us,
        )


def run_benchmarks(taxonomy: list[TestTaxon], num_iterations: int = 5) -> list[BenchmarkResult]:
    """Run benchmarks for multiple iterations."""
    results = []

    print(f'Taxonomy size: {len(taxonomy)} taxa', file=sys.stderr)
    print(f'Running {num_iterations} iterations...', file=sys.stderr)

    for i in range(num_iterations):
        result = run_benchmark(taxonomy)
        results.append(result)
        print(f'  Iteration {i + 1}: {result.wall_time_seconds:.3f}s', file=sys.stderr)

    return results


def print_results(results: list[BenchmarkResult]) -> None:
    """Print benchmark results as a formatted table."""
    if not results:
        print('No results to display')
        return

    print('\n' + '=' * 60)
    print('BENCHMARK RESULTS')
    print('=' * 60)

    best = min(results, key=lambda r: r.wall_time_seconds)
    worst = max(results, key=lambda r: r.wall_time_seconds)
    avg_time = sum(r.wall_time_seconds for r in results) / len(results)
    avg_memory = sum(r.peak_memory_mb for r in results) / len(results)

    print(f'Taxa count:     {best.num_taxa}')
    print(f'Iterations:     {len(results)}')
    print('-' * 60)
    print(f'Best time:      {best.wall_time_seconds:.3f}s ({best.time_per_taxon_us:.1f} µs/taxon)')
    print(f'Worst time:     {worst.wall_time_seconds:.3f}s')
    print(f'Average time:   {avg_time:.3f}s')
    print(f'Average memory: {avg_memory:.1f} MB')
    print('=' * 60)


def main():
    parser = argparse.ArgumentParser(description='Benchmark taxonomy aggregation')
    parser.add_argument(
        '--iterations',
        type=int,
        default=5,
        help='Number of benchmark iterations (default: 5)',
    )
    args = parser.parse_args()
    taxonomy = load_taxonomy_from_csv()
    results = run_benchmarks(taxonomy, args.iterations)
    print_results(results)


if __name__ == '__main__':
    main()
