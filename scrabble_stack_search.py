from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable


DB_PATH = Path(r"C:\Users\james\ScrabLab\resources\CSW24.db")
RACK_SIZE = 7
DEFAULT_TARGET_ROWS = 7
DEFAULT_RESULTS_DIR = Path("results")


@dataclass(frozen=True)
class FoundGrid:
    rows: tuple[str, ...]
    play_order: tuple[tuple[int, str, str], ...]


@dataclass(frozen=True)
class ResultPaths:
    txt_path: Path
    jsonl_path: Path
    summary_path: Path


class SearchData:
    def __init__(
        self,
        db_path: Path,
        target_rows: int,
        row_limit: int | None = None,
        min_playability: int | None = None,
    ) -> None:
        self.db_path = db_path
        self.target_rows = target_rows
        self.row_limit = row_limit
        self.min_playability = min_playability

        self.row_words: list[str] = []
        self.row_scores: list[int] = []
        self.row_index_by_pos: list[dict[str, set[int]]] = [
            defaultdict(set) for _ in range(RACK_SIZE)
        ]
        self.back_extensions: list[dict[str, tuple[str, ...]]] = [
            {} for _ in range(self.target_rows)
        ]
        self.front_extensions: list[dict[str, tuple[str, ...]]] = [
            {} for _ in range(self.target_rows)
        ]

        self._load()

    def _load(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        params: list[int] = [RACK_SIZE]
        where = ["length = ?"]
        if self.min_playability is not None:
            where.append("playability >= ?")
            params.append(self.min_playability)

        query = f"""
            SELECT word, COALESCE(playability, 0)
            FROM words
            WHERE {' AND '.join(where)}
            ORDER BY playability DESC, playability_order ASC, word ASC
        """
        if self.row_limit is not None:
            query += " LIMIT ?"
            params.append(self.row_limit)

        for word, playability in cur.execute(query, params):
            word = word.upper()
            if len(word) != RACK_SIZE or not word.isalpha():
                continue
            row_id = len(self.row_words)
            self.row_words.append(word)
            self.row_scores.append(int(playability or 0))
            for pos, letter in enumerate(word):
                self.row_index_by_pos[pos][letter].add(row_id)

        for length in range(2, self.target_rows + 1):
            front_map: dict[str, set[str]] = defaultdict(set)
            back_map: dict[str, set[str]] = defaultdict(set)
            for (word,) in cur.execute(
                "SELECT word FROM words WHERE length = ?",
                (length,),
            ):
                word = word.upper()
                if not word.isalpha():
                    continue
                front_map[word[1:]].add(word[0])
                back_map[word[:-1]].add(word[-1])

            key_length = length - 1
            self.front_extensions[key_length] = {
                key: tuple(sorted(value))
                for key, value in front_map.items()
            }
            self.back_extensions[key_length] = {
                key: tuple(sorted(value))
                for key, value in back_map.items()
            }

        conn.close()

    def stats(self) -> str:
        return (
            f"{len(self.row_words):,} candidate {RACK_SIZE}-letter row words loaded "
            f"for a {self.target_rows}x{RACK_SIZE} search"
        )


DATA: SearchData | None = None
BEST_DEPTH_REPORTED = 0


def init_worker(
    db_path: str,
    target_rows: int,
    row_limit: int | None,
    min_playability: int | None,
) -> None:
    global DATA
    DATA = SearchData(
        Path(db_path),
        target_rows=target_rows,
        row_limit=row_limit,
        min_playability=min_playability,
    )


def candidate_ids_for_allowed_letters(
    data: SearchData,
    allowed_letters: tuple[tuple[str, ...], ...],
) -> list[int]:
    base_ids: set[int] | None = None

    for pos, letters in enumerate(allowed_letters):
        if not letters:
            return []

        union_ids: set[int] = set()
        for letter in letters:
            union_ids |= data.row_index_by_pos[pos].get(letter, set())

        if not union_ids:
            return []

        if base_ids is None or len(union_ids) < len(base_ids):
            base_ids = union_ids
        else:
            base_ids &= union_ids

        if not base_ids:
            return []

    assert base_ids is not None

    def matches(word_id: int) -> bool:
        word = data.row_words[word_id]
        return all(word[pos] in allowed for pos, allowed in enumerate(allowed_letters))

    return sorted(
        (word_id for word_id in base_ids if matches(word_id)),
        key=lambda word_id: (-data.row_scores[word_id], data.row_words[word_id]),
    )


def format_grid(rows: Iterable[str]) -> str:
    row_list = list(rows)
    if not row_list:
        return ""

    width = len(row_list[0])
    border = "+" + "---+" * width
    lines = [border]
    for row in row_list:
        lines.append("| " + " | ".join(row) + " |")
        lines.append(border)
    return "\n".join(lines)


def print_solution(found: FoundGrid, solution_number: int) -> None:
    print(f"\n=== Solution {solution_number} ===")
    print(format_grid(found.rows))
    print("Play order:")
    for turn, side, word in found.play_order:
        print(f"  {turn}. {side:<6} {word}")


def print_partial_grid(
    rows: tuple[str, ...],
    play_order: tuple[tuple[int, str, str], ...],
    depth: int,
) -> None:
    print(f"\n--- First grid reaching {depth} row(s) ---")
    print(format_grid(rows))
    print("Play order so far:")
    for turn, side, word in play_order:
        print(f"  {turn}. {side:<6} {word}")


def search_from_start(
    start_word_id: int,
    limit: int | None,
    report_partial_depths: bool = False,
) -> list[FoundGrid]:
    assert DATA is not None
    data = DATA
    global BEST_DEPTH_REPORTED

    start_word = data.row_words[start_word_id]
    dead_states: set[tuple[str, ...]] = set()
    found: list[FoundGrid] = []

    @lru_cache(maxsize=500_000)
    def next_candidates(
        direction: str,
        columns: tuple[str, ...],
    ) -> tuple[int, ...]:
        height = len(columns[0])
        if height >= data.target_rows:
            return ()

        if direction == "top":
            extension_map = data.front_extensions[height]
        else:
            extension_map = data.back_extensions[height]

        allowed: list[tuple[str, ...]] = []
        for column in columns:
            letters = extension_map.get(column)
            if not letters:
                return ()
            allowed.append(letters)

        return tuple(
            candidate_ids_for_allowed_letters(data, tuple(allowed))
        )

    def dfs(
        rows: tuple[str, ...],
        columns: tuple[str, ...],
        play_order: tuple[tuple[int, str, str], ...],
    ) -> bool:
        global BEST_DEPTH_REPORTED
        if limit is not None and len(found) >= limit:
            return True

        height = len(rows)
        if report_partial_depths and height >= 2 and height > BEST_DEPTH_REPORTED:
            BEST_DEPTH_REPORTED = height
            print_partial_grid(rows, play_order, height)

        if height == data.target_rows:
            found.append(FoundGrid(rows=rows, play_order=play_order))
            return limit is not None and len(found) >= limit

        if columns in dead_states:
            return False

        for direction in ("top", "bottom"):
            candidate_ids = next_candidates(direction, columns)
            for word_id in candidate_ids:
                word = data.row_words[word_id]
                if direction == "top":
                    new_rows = (word,) + rows
                    new_columns = tuple(word[col] + columns[col] for col in range(RACK_SIZE))
                else:
                    new_rows = rows + (word,)
                    new_columns = tuple(columns[col] + word[col] for col in range(RACK_SIZE))

                new_play_order = play_order + ((height + 1, direction, word),)

                if dfs(new_rows, new_columns, new_play_order):
                    return True

        dead_states.add(columns)
        return False

    initial_columns = tuple(letter for letter in start_word)
    dfs(
        rows=(start_word,),
        columns=initial_columns,
        play_order=((1, "start", start_word),),
    )
    return found


def worker_task(args: tuple[int, int | None]) -> tuple[int, list[FoundGrid]]:
    start_word_id, limit = args
    results = search_from_start(start_word_id, limit)
    return start_word_id, results


def solution_to_dict(solution_number: int, found: FoundGrid) -> dict[str, object]:
    return {
        "solution_number": solution_number,
        "rows": list(found.rows),
        "play_order": [
            {"turn": turn, "side": side, "word": word}
            for turn, side, word in found.play_order
        ],
    }


class ResultWriter:
    def __init__(self, args: argparse.Namespace, total_starts: int) -> None:
        self.args = args
        self.total_starts = total_starts
        self.paths = self._build_paths()
        self.pending_txt_blocks: list[str] = []
        self.pending_jsonl_lines: list[str] = []
        self.solution_count = 0

        self.args.results_dir.mkdir(parents=True, exist_ok=True)
        self.paths.txt_path.write_text(self._header_text(), encoding="utf-8")
        self.paths.jsonl_path.write_text("", encoding="utf-8")

    def _build_paths(self) -> ResultPaths:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        stem = f"rows_{self.args.rows}_limit_{self.args.limit or 'all'}_{timestamp}"
        return ResultPaths(
            txt_path=self.args.results_dir / f"{stem}.txt",
            jsonl_path=self.args.results_dir / f"{stem}.jsonl",
            summary_path=self.args.results_dir / f"{stem}_summary.json",
        )

    def _header_text(self) -> str:
        lines = [
            "Scrabble stack search results",
            f"Target rows: {self.args.rows}",
            f"Rack size: {RACK_SIZE}",
            f"DB path: {self.args.db_path}",
            f"Limit: {self.args.limit if self.args.limit is not None else 'all'}",
            f"Row limit: {self.args.row_limit if self.args.row_limit is not None else 'all'}",
            f"Max start words: {self.args.max_start_words if self.args.max_start_words is not None else 'all'}",
            f"Processes: {self.args.processes}",
            "",
        ]
        return "\n".join(lines)

    def append_solution(self, found: FoundGrid) -> None:
        self.solution_count += 1
        self.pending_txt_blocks.append(self._solution_text(self.solution_count, found))
        self.pending_jsonl_lines.append(
            json.dumps(solution_to_dict(self.solution_count, found))
        )

        if self.solution_count % self.args.append_every == 0:
            self.flush()

    def _solution_text(self, solution_number: int, found: FoundGrid) -> str:
        lines = [
            f"=== Solution {solution_number} ===",
            format_grid(found.rows),
            "Play order:",
        ]
        for turn, side, word in found.play_order:
            lines.append(f"  {turn}. {side:<6} {word}")
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

    def finish(self, *, elapsed_seconds: float, processed: int) -> None:
        self.flush()

        summary = {
            "db_path": str(self.args.db_path),
            "target_rows": self.args.rows,
            "rack_size": RACK_SIZE,
            "limit": self.args.limit,
            "row_limit": self.args.row_limit,
            "min_playability": self.args.min_playability,
            "max_start_words": self.args.max_start_words,
            "processes": self.args.processes,
            "progress_every": self.args.progress_every,
            "report_partial_depths": self.args.report_partial_depths,
            "print_solutions": not self.args.quiet_solutions,
            "append_every": self.args.append_every,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "processed_start_words": processed,
            "total_start_words": self.total_starts,
            "solution_count": self.solution_count,
            "text_results_path": str(self.paths.txt_path),
            "jsonl_results_path": str(self.paths.jsonl_path),
        }
        self.paths.summary_path.write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Depth-first search for Scrabble stacks where each turn adds a full "
            "7-letter row to the current top or bottom and every vertical cross-word "
            "formed on that turn is valid."
        )
    )
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_TARGET_ROWS,
        help="Number of stacked rows to search for. Default is 7.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Stop after this many full grids. Use 0 for no limit.",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Only consider this many 7-letter row words, ordered by playability.",
    )
    parser.add_argument(
        "--min-playability",
        type=int,
        default=None,
        help="Optional minimum playability filter for 7-letter row words.",
    )
    parser.add_argument(
        "--max-start-words",
        type=int,
        default=None,
        help="Only try this many starting rows from the ordered 7-letter list.",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of CPU processes to use. GPU is not used by this script.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Print a progress message after this many starting words.",
    )
    parser.add_argument(
        "--report-partial-depths",
        action="store_true",
        help=(
            "Print the first partial grid found at each new row depth "
            "(2 rows, 3 rows, and so on). Best used with --processes 1."
        ),
    )
    parser.add_argument(
        "--quiet-solutions",
        action="store_true",
        help="Do not print full solutions to the shell. Results are still saved to disk.",
    )
    parser.add_argument(
        "--append-every",
        type=int,
        default=1000,
        help="Flush saved solutions to disk after this many finds.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Folder where text, JSONL, and summary files will be saved.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows < 2 or args.rows > RACK_SIZE:
        raise SystemExit(f"--rows must be between 2 and {RACK_SIZE}.")
    if args.append_every < 1:
        raise SystemExit("--append-every must be at least 1.")
    if args.limit == 0:
        args.limit = None

    started = time.time()

    global DATA, BEST_DEPTH_REPORTED
    BEST_DEPTH_REPORTED = 0
    DATA = SearchData(
        args.db_path,
        target_rows=args.rows,
        row_limit=args.row_limit,
        min_playability=args.min_playability,
    )
    print(DATA.stats())

    total_starts = len(DATA.row_words)
    if args.max_start_words is not None:
        total_starts = min(total_starts, args.max_start_words)

    if total_starts == 0:
        print("No candidate starting words matched the filters.")
        return

    processed = 0
    writer = ResultWriter(args, total_starts)
    print(f"Saving text results to {writer.paths.txt_path}")
    print(f"Saving JSONL results to {writer.paths.jsonl_path}")

    if args.processes <= 1:
        for start_word_id in range(total_starts):
            results = search_from_start(
                start_word_id,
                args.limit - writer.solution_count if args.limit else None,
                report_partial_depths=args.report_partial_depths,
            )
            processed += 1

            for found in results:
                writer.append_solution(found)
                if not args.quiet_solutions:
                    print_solution(found, writer.solution_count)
                if args.limit and writer.solution_count >= args.limit:
                    elapsed = time.time() - started
                    writer.finish(elapsed_seconds=elapsed, processed=processed)
                    print(f"Saved summary to {writer.paths.summary_path}")
                    print(f"\nStopped after {writer.solution_count} solution(s) in {elapsed:.1f}s.")
                    return

            if args.progress_every and processed % args.progress_every == 0:
                elapsed = time.time() - started
                print(
                    f"Processed {processed:,}/{total_starts:,} starting rows "
                    f"in {elapsed:.1f}s, found {writer.solution_count} solution(s)."
                )
    else:
        if args.report_partial_depths:
            print(
                "Partial-depth reporting is only enabled in single-process mode, "
                "so it will be skipped with --processes > 1."
            )
        tasks = [(start_word_id, args.limit) for start_word_id in range(total_starts)]
        with mp.Pool(
            processes=args.processes,
            initializer=init_worker,
            initargs=(
                str(args.db_path),
                args.rows,
                args.row_limit,
                args.min_playability,
            ),
        ) as pool:
            for start_word_id, results in pool.imap_unordered(worker_task, tasks, chunksize=1):
                processed += 1

                for found in results:
                    writer.append_solution(found)
                    if not args.quiet_solutions:
                        print_solution(found, writer.solution_count)
                    if args.limit and writer.solution_count >= args.limit:
                        pool.terminate()
                        elapsed = time.time() - started
                        writer.finish(elapsed_seconds=elapsed, processed=processed)
                        print(f"Saved summary to {writer.paths.summary_path}")
                        print(f"\nStopped after {writer.solution_count} solution(s) in {elapsed:.1f}s.")
                        return

                if args.progress_every and processed % args.progress_every == 0:
                    elapsed = time.time() - started
                    start_word = DATA.row_words[start_word_id]
                    print(
                        f"Processed {processed:,}/{total_starts:,} starting rows "
                        f"(latest start: {start_word}) in {elapsed:.1f}s, "
                        f"found {writer.solution_count} solution(s)."
                    )

    elapsed = time.time() - started
    writer.finish(elapsed_seconds=elapsed, processed=processed)
    print(f"Saved summary to {writer.paths.summary_path}")
    print(f"\nFinished in {elapsed:.1f}s. Found {writer.solution_count} solution(s).")


if __name__ == "__main__":
    mp.freeze_support()
    main()
