from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


DB_PATH = Path(r"C:\Users\james\ScrabLab\resources\CSW24.db")
GRID_ROWS = 8
GRID_COLS = 7
DEFAULT_RESULTS_DIR = Path("results")


@dataclass(frozen=True)
class Solution:
    anchor_row: int
    scaffold: str
    rows: tuple[str, ...]
    columns: tuple[str, ...]
    play_order: tuple[tuple[int, int, str], ...]


@dataclass(frozen=True)
class ResultPaths:
    txt_path: Path
    jsonl_path: Path
    summary_path: Path
    checkpoint_path: Path


class SearchData:
    def __init__(
        self,
        db_path: Path,
        scaffold_limit: int | None = None,
        min_scaffold_playability: int | None = None,
    ) -> None:
        self.db_path = db_path
        self.scaffold_limit = scaffold_limit
        self.min_scaffold_playability = min_scaffold_playability

        self.valid_words_by_length: dict[int, set[str]] = {}
        self.row_words: list[str] = []
        self.row_scores: list[int] = []
        self.row_index_by_pos: list[dict[str, set[int]]] = [
            defaultdict(set) for _ in range(GRID_COLS)
        ]
        self.vertical_candidates: list[dict[str, list[str]]] = [
            defaultdict(list) for _ in range(GRID_ROWS)
        ]

        self._load()

    def _load(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        for length in range(2, GRID_ROWS + 1):
            words = {
                word.upper()
                for (word,) in cur.execute(
                    "SELECT word FROM words WHERE length = ?",
                    (length,),
                )
                if word.isalpha()
            }
            self.valid_words_by_length[length] = words

        params: list[int] = [GRID_COLS]
        where = ["length = ?"]
        if self.min_scaffold_playability is not None:
            where.append("playability >= ?")
            params.append(self.min_scaffold_playability)

        query = f"""
            SELECT word, COALESCE(playability, 0)
            FROM words
            WHERE {' AND '.join(where)}
            ORDER BY playability DESC, playability_order ASC, word ASC
        """
        if self.scaffold_limit is not None:
            query += " LIMIT ?"
            params.append(self.scaffold_limit)

        for word, playability in cur.execute(query, params):
            word = word.upper()
            if len(word) != GRID_COLS or not word.isalpha():
                continue
            row_id = len(self.row_words)
            self.row_words.append(word)
            self.row_scores.append(int(playability or 0))
            for pos, letter in enumerate(word):
                self.row_index_by_pos[pos][letter].add(row_id)

        for word, _playability in cur.execute(
            """
            SELECT word, COALESCE(playability, 0)
            FROM words
            WHERE length = ?
            ORDER BY playability DESC, playability_order ASC, word ASC
            """,
            (GRID_ROWS,),
        ):
            word = word.upper()
            if len(word) != GRID_ROWS or not word.isalpha():
                continue
            for anchor_row in range(GRID_ROWS):
                self.vertical_candidates[anchor_row][word[anchor_row]].append(word)

        conn.close()

    def stats(self) -> str:
        return (
            f"{len(self.row_words):,} scaffold words loaded, "
            f"{sum(len(group) for group in self.vertical_candidates[0].values()):,} "
            f"vertical 8-letter words loaded"
        )


DATA: SearchData | None = None


def init_worker(
    db_path: str,
    scaffold_limit: int | None,
    min_scaffold_playability: int | None,
) -> None:
    global DATA
    DATA = SearchData(
        Path(db_path),
        scaffold_limit=scaffold_limit,
        min_scaffold_playability=min_scaffold_playability,
    )


def worker_task(job: tuple[str, int, int | None]) -> tuple[str, int, list[Solution]]:
    assert DATA is not None
    scaffold, anchor_row, limit = job
    return scaffold, anchor_row, search_for_scaffold(DATA, scaffold, anchor_row, limit)


def solution_to_dict(solution_number: int, solution: Solution) -> dict[str, object]:
    return {
        "solution_number": solution_number,
        "anchor_row": solution.anchor_row + 1,
        "scaffold": solution.scaffold,
        "rows": list(solution.rows),
        "columns": list(solution.columns),
        "play_order": [
            {"turn": turn, "column": column_index + 1, "word": word}
            for turn, column_index, word in solution.play_order
        ],
    }


class ResultWriter:
    def __init__(
        self,
        *,
        results_dir: Path,
        stem: str,
        args: argparse.Namespace,
        total_jobs: int,
        resume_paths: ResultPaths | None = None,
        initial_solution_count: int = 0,
    ) -> None:
        self.results_dir = results_dir
        self.stem = stem
        self.args = args
        self.total_jobs = total_jobs
        self.solution_count = initial_solution_count
        self.pending_txt_blocks: list[str] = []
        self.pending_jsonl_lines: list[str] = []

        self.results_dir.mkdir(parents=True, exist_ok=True)
        if resume_paths is None:
            self.paths = ResultPaths(
                txt_path=self.results_dir / f"{stem}.txt",
                jsonl_path=self.results_dir / f"{stem}.jsonl",
                summary_path=self.results_dir / f"{stem}_summary.json",
                checkpoint_path=self.results_dir / f"{stem}_checkpoint.json",
            )
            self.paths.txt_path.write_text(self._header_text(), encoding="utf-8")
            self.paths.jsonl_path.write_text("", encoding="utf-8")
        else:
            self.paths = resume_paths

    def _header_text(self) -> str:
        lines = [
            "Scaffold rectangle search results",
            f"DB path: {self.args.db_path}",
            f"Limit: {self.args.limit if self.args.limit is not None else 'all'}",
            f"Scaffold limit: {self.args.scaffold_limit if self.args.scaffold_limit is not None else 'all'}",
            f"Max scaffolds: {self.args.max_scaffolds if self.args.max_scaffolds is not None else 'all'}",
            f"Anchor row: {self.args.anchor_row if self.args.anchor_row is not None else 'all'}",
            f"Processes: {self.args.processes}",
            "",
        ]
        return "\n".join(lines)

    def append_solution(self, solution: Solution) -> None:
        self.solution_count += 1
        self.pending_txt_blocks.append(self._solution_text(self.solution_count, solution))
        self.pending_jsonl_lines.append(json.dumps(solution_to_dict(self.solution_count, solution)))

        if self.solution_count % self.args.append_every == 0:
            self.flush()

    def _solution_text(self, solution_number: int, solution: Solution) -> str:
        lines = [
            f"=== Solution {solution_number} ===",
            f"Scaffold row: {solution.anchor_row + 1}",
            f"Scaffold word: {solution.scaffold}",
            format_grid(solution.rows),
            "Play order:",
            f"  1. scaffold row {solution.anchor_row + 1}: {solution.scaffold}",
        ]
        for turn, column_index, word in solution.play_order:
            lines.append(f"  {turn}. column {column_index + 1}: {word}")
        lines.append("")
        return "\n".join(lines)

    def flush(self) -> None:
        if self.pending_txt_blocks:
            with self.paths.txt_path.open("a", encoding="utf-8") as handle:
                for block in self.pending_txt_blocks:
                    handle.write(block)
                    handle.write("\n")
            self.pending_txt_blocks.clear()

        if self.pending_jsonl_lines:
            with self.paths.jsonl_path.open("a", encoding="utf-8") as handle:
                for line in self.pending_jsonl_lines:
                    handle.write(line)
                    handle.write("\n")
            self.pending_jsonl_lines.clear()

    def write_checkpoint(
        self,
        *,
        completed_jobs: list[dict[str, object]],
        elapsed_seconds: float,
        status: str,
    ) -> None:
        self.flush()
        checkpoint = {
            "stem": self.stem,
            "db_path": str(self.args.db_path),
            "limit": self.args.limit,
            "scaffold_limit": self.args.scaffold_limit,
            "min_scaffold_playability": self.args.min_scaffold_playability,
            "max_scaffolds": self.args.max_scaffolds,
            "anchor_row": self.args.anchor_row,
            "progress_every": self.args.progress_every,
            "processes": self.args.processes,
            "append_every": self.args.append_every,
            "quiet_solutions": self.args.quiet_solutions,
            "results_dir": str(self.results_dir),
            "stop_file": str(self.args.stop_file) if self.args.stop_file is not None else None,
            "status": status,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "total_jobs": self.total_jobs,
            "solution_count": self.solution_count,
            "completed_jobs": completed_jobs,
            "txt_path": str(self.paths.txt_path),
            "jsonl_path": str(self.paths.jsonl_path),
            "summary_path": str(self.paths.summary_path),
        }
        self.paths.checkpoint_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")

    def write_summary(
        self,
        *,
        elapsed_seconds: float,
        processed_jobs: int,
        status: str,
    ) -> None:
        self.flush()
        summary = {
            "db_path": str(self.args.db_path),
            "limit": self.args.limit,
            "scaffold_limit": self.args.scaffold_limit,
            "min_scaffold_playability": self.args.min_scaffold_playability,
            "max_scaffolds": self.args.max_scaffolds,
            "anchor_row": self.args.anchor_row,
            "progress_every": self.args.progress_every,
            "processes": self.args.processes,
            "append_every": self.args.append_every,
            "quiet_solutions": self.args.quiet_solutions,
            "stop_file": str(self.args.stop_file) if self.args.stop_file is not None else None,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "processed_jobs": processed_jobs,
            "total_jobs": self.total_jobs,
            "solution_count": self.solution_count,
            "status": status,
            "text_results_path": str(self.paths.txt_path),
            "jsonl_results_path": str(self.paths.jsonl_path),
            "checkpoint_path": str(self.paths.checkpoint_path),
        }
        self.paths.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def format_grid(rows: tuple[str, ...]) -> str:
    border = "+" + "---+" * GRID_COLS
    lines = [border]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
        lines.append(border)
    return "\n".join(lines)


def print_solution(solution: Solution, number: int) -> None:
    print(f"\n=== Solution {number} ===")
    print(f"Scaffold row: {solution.anchor_row + 1}")
    print(f"Scaffold word: {solution.scaffold}")
    print(format_grid(solution.rows))
    print("Play order:")
    print(f"  1. scaffold row {solution.anchor_row + 1}: {solution.scaffold}")
    for turn, column_index, word in solution.play_order:
        print(f"  {turn}. column {column_index + 1}: {word}")


def contiguous_runs_valid(cells: list[str | None], valid_words_by_length: dict[int, set[str]]) -> bool:
    run: list[str] = []
    for cell in cells + [None]:
        if cell is None:
            if len(run) >= 2:
                word = "".join(run)
                if word not in valid_words_by_length[len(run)]:
                    return False
            run = []
        else:
            run.append(cell)
    return True


def build_row_patterns(grid: list[list[str | None]]) -> tuple[str, ...]:
    patterns = []
    for row in grid:
        patterns.append("".join(cell if cell is not None else "?" for cell in row))
    return tuple(patterns)


def pattern_matches_word(data: SearchData, pattern: str) -> bool:
    fixed_positions = [(index, letter) for index, letter in enumerate(pattern) if letter != "?"]
    if not fixed_positions:
        return bool(data.row_words)

    candidate_ids: set[int] | None = None
    for pos, letter in fixed_positions:
        ids = data.row_index_by_pos[pos].get(letter, set())
        if not ids:
            return False
        if candidate_ids is None or len(ids) < len(candidate_ids):
            candidate_ids = set(ids)
        else:
            candidate_ids &= ids
        if not candidate_ids:
            return False
    return True


def with_letter(pattern: str, column_index: int, letter: str) -> str:
    return pattern[:column_index] + letter + pattern[column_index + 1 :]


def rows_from_patterns(row_patterns: tuple[str, ...]) -> tuple[str, ...]:
    return row_patterns


def columns_from_patterns(row_patterns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple("".join(row_patterns[row][col] for row in range(GRID_ROWS)) for col in range(GRID_COLS))


def job_key(scaffold: str, anchor_row: int) -> str:
    return f"{scaffold}|{anchor_row}"


def parse_job_key(value: str) -> tuple[str, int]:
    scaffold, anchor_row_text = value.rsplit("|", 1)
    return scaffold, int(anchor_row_text)


def make_run_stem() -> str:
    return f"scaffold_search_{time.strftime('%Y%m%d_%H%M%S')}"


def load_checkpoint(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def cli_has_option(*option_names: str) -> bool:
    argv = sys.argv[1:]
    return any(option in argv for option in option_names)


def search_for_scaffold(
    data: SearchData,
    scaffold: str,
    anchor_row: int,
    limit: int | None,
) -> list[Solution]:
    initial_patterns = tuple(
        scaffold if row == anchor_row else "?" * GRID_COLS
        for row in range(GRID_ROWS)
    )
    found: list[Solution] = []
    dead_states: set[tuple[int, tuple[str, ...]]] = set()
    seen_complete: set[tuple[str, ...]] = set()

    @lru_cache(maxsize=500_000)
    def row_pattern_has_match(pattern: str) -> bool:
        return pattern_matches_word(data, pattern)

    @lru_cache(maxsize=1_000_000)
    def row_pattern_is_valid(pattern: str) -> bool:
        if not contiguous_runs_valid(
            [None if char == "?" else char for char in pattern],
            data.valid_words_by_length,
        ):
            return False
        return row_pattern_has_match(pattern)

    def candidate_words_for_column(
        column_index: int,
        row_patterns: tuple[str, ...],
    ) -> list[tuple[str, tuple[str, ...]]]:
        target_letter = scaffold[column_index]
        candidates = data.vertical_candidates[anchor_row][target_letter]
        viable: list[tuple[str, tuple[str, ...]]] = []

        for word in candidates:
            next_patterns = list(row_patterns)
            valid = True
            for row_index, letter in enumerate(word):
                if row_index == anchor_row:
                    continue
                updated_pattern = with_letter(row_patterns[row_index], column_index, letter)
                if not row_pattern_is_valid(updated_pattern):
                    valid = False
                    break
                next_patterns[row_index] = updated_pattern

            if valid:
                viable.append((word, tuple(next_patterns)))

        return viable

    def dfs(
        filled_mask: int,
        row_patterns: tuple[str, ...],
        placed_columns: tuple[str | None, ...],
        play_order: tuple[tuple[int, int, str], ...],
    ) -> bool:
        if limit is not None and len(found) >= limit:
            return True

        key = (filled_mask, row_patterns)
        if filled_mask == (1 << GRID_COLS) - 1:
            rows = rows_from_patterns(row_patterns)
            if rows not in seen_complete:
                seen_complete.add(rows)
                found.append(
                    Solution(
                        anchor_row=anchor_row,
                        scaffold=scaffold,
                        rows=rows,
                        columns=columns_from_patterns(row_patterns),
                        play_order=play_order,
                    )
                )
            return limit is not None and len(found) >= limit

        if key in dead_states:
            return False

        best_column: int | None = None
        best_candidates: list[tuple[str, tuple[str, ...]]] | None = None

        for column_index in range(GRID_COLS):
            if filled_mask & (1 << column_index):
                continue
            candidates = candidate_words_for_column(column_index, row_patterns)
            if not candidates:
                dead_states.add(key)
                return False
            if best_candidates is None or len(candidates) < len(best_candidates):
                best_column = column_index
                best_candidates = candidates

        assert best_column is not None and best_candidates is not None

        turn_number = 2 + len(play_order)
        for word, next_patterns in best_candidates:
            next_columns = list(placed_columns)
            next_columns[best_column] = word

            if dfs(
                filled_mask | (1 << best_column),
                next_patterns,
                tuple(next_columns),
                play_order + ((turn_number, best_column, word),),
            ):
                return True

        dead_states.add(key)
        return False

    dfs(
        0,
        initial_patterns,
        tuple([None] * GRID_COLS),
        tuple(),
    )
    return found


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Search for 8x7 scaffold grids: a 7-letter scaffold row is played first, "
            "then seven 8-letter vertical words are added one column at a time. "
            "After each placement, every contiguous horizontal run of length 2 or more "
            "must be a valid word."
        )
    )
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Stop after this many complete grids. Use 0 for no limit.",
    )
    parser.add_argument(
        "--scaffold-limit",
        type=int,
        default=None,
        help="Only load this many 7-letter scaffold words, ordered by playability.",
    )
    parser.add_argument(
        "--min-scaffold-playability",
        type=int,
        default=None,
        help="Optional minimum playability filter for scaffold words.",
    )
    parser.add_argument(
        "--max-scaffolds",
        type=int,
        default=None,
        help="Only try this many scaffold words from the loaded list.",
    )
    parser.add_argument(
        "--anchor-row",
        type=int,
        default=None,
        help="Only search one scaffold row position, from 1 to 8.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Print progress after this many scaffold/row jobs.",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of CPU processes to use across scaffold jobs.",
    )
    parser.add_argument(
        "--quiet-solutions",
        action="store_true",
        help="Do not print full grids to the shell.",
    )
    parser.add_argument(
        "--print-solutions",
        action="store_true",
        help="Print full grids to the shell, including on resumed runs.",
    )
    parser.add_argument(
        "--append-every",
        type=int,
        default=100,
        help="Flush saved solutions to disk after this many finds.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Folder where result and checkpoint files will be saved.",
    )
    parser.add_argument(
        "--resume",
        type=Path,
        default=None,
        help="Resume from a checkpoint JSON file created by this script.",
    )
    parser.add_argument(
        "--stop-file",
        type=Path,
        default=None,
        help="If this file exists, stop after the current finished job and write a checkpoint.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=1,
        help="Write checkpoint progress after this many completed jobs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cli_stop_file = args.stop_file
    checkpoint_data: dict[str, object] | None = None
    if args.resume is not None:
        checkpoint_data = load_checkpoint(args.resume)
        if not cli_has_option("--db-path"):
            args.db_path = Path(checkpoint_data["db_path"])
        if not cli_has_option("--limit"):
            args.limit = checkpoint_data["limit"]
        if not cli_has_option("--scaffold-limit"):
            args.scaffold_limit = checkpoint_data["scaffold_limit"]
        if not cli_has_option("--min-scaffold-playability"):
            args.min_scaffold_playability = checkpoint_data["min_scaffold_playability"]
        if not cli_has_option("--max-scaffolds"):
            args.max_scaffolds = checkpoint_data["max_scaffolds"]
        if not cli_has_option("--anchor-row"):
            args.anchor_row = checkpoint_data["anchor_row"]
        if not cli_has_option("--progress-every"):
            args.progress_every = checkpoint_data["progress_every"]
        if not cli_has_option("--processes"):
            args.processes = checkpoint_data["processes"]
        if not cli_has_option("--append-every"):
            args.append_every = checkpoint_data["append_every"]
        if cli_has_option("--print-solutions"):
            args.quiet_solutions = False
        elif not cli_has_option("--quiet-solutions"):
            args.quiet_solutions = checkpoint_data["quiet_solutions"]
        if not cli_has_option("--results-dir"):
            args.results_dir = Path(checkpoint_data["results_dir"])
        checkpoint_stop_file = checkpoint_data.get("stop_file")
        args.stop_file = Path(checkpoint_stop_file) if checkpoint_stop_file else None

    if cli_stop_file is not None:
        args.stop_file = cli_stop_file
    if args.print_solutions:
        args.quiet_solutions = False

    if args.limit == 0:
        args.limit = None
    if args.anchor_row is not None and not 1 <= args.anchor_row <= GRID_ROWS:
        raise SystemExit("--anchor-row must be between 1 and 8.")
    if args.processes < 1:
        raise SystemExit("--processes must be at least 1.")
    if args.append_every < 1:
        raise SystemExit("--append-every must be at least 1.")
    if args.checkpoint_every < 1:
        raise SystemExit("--checkpoint-every must be at least 1.")

    started = time.time()
    data = SearchData(
        args.db_path,
        scaffold_limit=args.scaffold_limit,
        min_scaffold_playability=args.min_scaffold_playability,
    )
    print(data.stats())

    scaffold_words = data.row_words
    if args.max_scaffolds is not None:
        scaffold_words = scaffold_words[: args.max_scaffolds]

    if not scaffold_words:
        print("No scaffold words matched the filters.")
        return

    anchor_rows = [args.anchor_row - 1] if args.anchor_row is not None else list(range(GRID_ROWS))
    total_jobs = len(scaffold_words) * len(anchor_rows)

    completed_jobs: list[dict[str, object]] = []
    completed_job_keys: set[str] = set()
    result_paths: ResultPaths | None = None
    stem = make_run_stem()
    initial_solution_count = 0

    if checkpoint_data is not None:
        completed_jobs = list(checkpoint_data.get("completed_jobs", []))
        completed_job_keys = {str(item["job_key"]) for item in completed_jobs}
        stem = str(checkpoint_data["stem"])
        initial_solution_count = int(checkpoint_data.get("solution_count", 0))
        result_paths = ResultPaths(
            txt_path=Path(checkpoint_data["txt_path"]),
            jsonl_path=Path(checkpoint_data["jsonl_path"]),
            summary_path=Path(checkpoint_data["summary_path"]),
            checkpoint_path=args.resume,
        )

    writer = ResultWriter(
        results_dir=args.results_dir,
        stem=stem,
        args=args,
        total_jobs=total_jobs,
        resume_paths=result_paths,
        initial_solution_count=initial_solution_count,
    )
    print(f"Saving text results to {writer.paths.txt_path}")
    print(f"Saving JSONL results to {writer.paths.jsonl_path}")
    print(f"Saving checkpoints to {writer.paths.checkpoint_path}")

    solutions_found = writer.solution_count
    processed_jobs = 0
    jobs = [
        (scaffold, anchor_row, args.limit)
        for scaffold in scaffold_words
        for anchor_row in anchor_rows
        if job_key(scaffold, anchor_row) not in completed_job_keys
    ]
    total_jobs_remaining = len(jobs)

    def handle_results(results: list[Solution]) -> bool:
        nonlocal solutions_found
        for solution in results:
            writer.append_solution(solution)
            solutions_found = writer.solution_count
            if not args.quiet_solutions:
                print_solution(solution, solutions_found)
            if args.limit is not None and solutions_found >= args.limit:
                return True
        return False

    def record_job_completion(scaffold: str, anchor_row: int) -> None:
        nonlocal processed_jobs
        processed_jobs += 1
        completed_jobs.append(
            {
                "job_key": job_key(scaffold, anchor_row),
                "scaffold": scaffold,
                "anchor_row": anchor_row,
            }
        )
        completed_job_keys.add(job_key(scaffold, anchor_row))

    def maybe_checkpoint(status: str) -> None:
        elapsed = time.time() - started
        if processed_jobs == 0:
            return
        if processed_jobs % args.checkpoint_every != 0 and status == "running":
            return
        writer.write_checkpoint(
            completed_jobs=completed_jobs,
            elapsed_seconds=elapsed,
            status=status,
        )

    def stop_requested() -> bool:
        return args.stop_file is not None and args.stop_file.exists()

    def run_serial_jobs() -> bool:
        nonlocal processed_jobs
        for scaffold, anchor_row, _limit in jobs:
            remaining = args.limit - solutions_found if args.limit is not None else None
            results = search_for_scaffold(data, scaffold, anchor_row, remaining)
            record_job_completion(scaffold, anchor_row)

            if handle_results(results):
                maybe_checkpoint("stopped")
                return True

            maybe_checkpoint("running")

            if stop_requested():
                maybe_checkpoint("paused")
                return True

            if args.progress_every and processed_jobs % args.progress_every == 0:
                elapsed = time.time() - started
                print(
                    f"Processed {processed_jobs:,}/{total_jobs_remaining:,} scaffold jobs "
                    f"in {elapsed:.1f}s, found {solutions_found} solution(s)."
                )
        return False

    if args.processes == 1:
        if run_serial_jobs():
            elapsed = time.time() - started
            writer.write_summary(
                elapsed_seconds=elapsed,
                processed_jobs=processed_jobs,
                status="paused" if stop_requested() else "stopped",
            )
            print(f"\nStopped after {solutions_found} solution(s) in {elapsed:.1f}s.")
            return
    else:
        try:
            with mp.Pool(
                processes=args.processes,
                initializer=init_worker,
                initargs=(
                    str(args.db_path),
                    args.scaffold_limit,
                    args.min_scaffold_playability,
                ),
            ) as pool:
                for scaffold, anchor_row, results in pool.imap_unordered(worker_task, jobs, chunksize=1):
                    record_job_completion(scaffold, anchor_row)

                    if handle_results(results):
                        pool.terminate()
                        elapsed = time.time() - started
                        writer.write_checkpoint(
                            completed_jobs=completed_jobs,
                            elapsed_seconds=elapsed,
                            status="stopped",
                        )
                        writer.write_summary(
                            elapsed_seconds=elapsed,
                            processed_jobs=processed_jobs,
                            status="stopped",
                        )
                        print(f"\nStopped after {solutions_found} solution(s) in {elapsed:.1f}s.")
                        return

                    maybe_checkpoint("running")

                    if stop_requested():
                        pool.terminate()
                        elapsed = time.time() - started
                        writer.write_checkpoint(
                            completed_jobs=completed_jobs,
                            elapsed_seconds=elapsed,
                            status="paused",
                        )
                        writer.write_summary(
                            elapsed_seconds=elapsed,
                            processed_jobs=processed_jobs,
                            status="paused",
                        )
                        print(f"\nPaused after {solutions_found} solution(s) in {elapsed:.1f}s.")
                        return

                    if args.progress_every and processed_jobs % args.progress_every == 0:
                        elapsed = time.time() - started
                        print(
                            f"Processed {processed_jobs:,}/{total_jobs_remaining:,} scaffold jobs "
                            f"in {elapsed:.1f}s, found {solutions_found} solution(s)."
                        )
        except (OSError, PermissionError) as exc:
            print(
                f"Multiprocessing could not start ({exc}). "
                f"Falling back to single-process mode."
            )
            if run_serial_jobs():
                elapsed = time.time() - started
                writer.write_summary(
                    elapsed_seconds=elapsed,
                    processed_jobs=processed_jobs,
                    status="paused" if stop_requested() else "stopped",
                )
                print(f"\nStopped after {solutions_found} solution(s) in {elapsed:.1f}s.")
                return

    elapsed = time.time() - started
    writer.write_checkpoint(
        completed_jobs=completed_jobs,
        elapsed_seconds=elapsed,
        status="finished",
    )
    writer.write_summary(
        elapsed_seconds=elapsed,
        processed_jobs=processed_jobs,
        status="finished",
    )
    print(f"\nFinished in {elapsed:.1f}s. Found {solutions_found} solution(s).")


if __name__ == "__main__":
    mp.freeze_support()
    main()
