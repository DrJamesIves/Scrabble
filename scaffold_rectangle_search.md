# `scaffold_rectangle_search.py`

## Purpose

This script searches for a different Scrabble structure: an `8 x 7` rectangle built from:

- one initial 7-letter horizontal scaffold word
- seven later vertical plays, one in each column
- each vertical play being an 8-letter word that crosses the scaffold

All seven vertical words must cross the scaffold on the same row so that the final shape is a full rectangle.

## Playability Rules

The script enforces the playable-turn interpretation discussed during development:

- the scaffold row is always played first
- vertical words are then added one column at a time
- the placement order of the columns is part of the search
- after each placement, every contiguous horizontal run of length `2` or more must be valid
- single-letter horizontal fragments are allowed
- each completed vertical play is an 8-letter word by construction

This means a final rectangle only counts if there exists at least one legal order in which the seven vertical words could have been played.

## Search Model

For each job, the script fixes:

- a 7-letter scaffold word
- a scaffold row position from `1` to `8`

It then uses depth-first search to place the seven vertical words column by column.

The search is not locked to left-to-right column order. It chooses among the remaining columns and only accepts placements that keep the intermediate board legal.

## Data Source

By default the script reads:

`C:\Users\james\ScrabLab\resources\CSW24.db`

It expects a `words` table containing the lexicon.

## Output

Solutions are printed as boxed grids unless `--quiet-solutions` is used.

Results are saved automatically into the `results` folder as:

- `.txt` for readable solutions
- `.jsonl` for one structured solution per line
- `_summary.json` for metadata
- `_checkpoint.json` for pause/resume

## Main Arguments

- `--limit N`
  Stop after `N` complete grids. Use `0` for no limit.

- `--scaffold-limit N`
  Only load the top `N` scaffold words from the lexicon.

- `--min-scaffold-playability N`
  Ignore scaffold words below this playability.

- `--max-scaffolds N`
  Only try the first `N` loaded scaffold words.

- `--anchor-row N`
  Only search one scaffold row, numbered `1` to `8`.

- `--progress-every N`
  Print progress every `N` scaffold jobs.

- `--processes N`
  Number of worker processes used across scaffold jobs.

- `--quiet-solutions`
  Suppress full grid printing in the shell.

- `--print-solutions`
  Force printing of full grids, including on resumed runs.

- `--append-every N`
  Flush saved solutions to disk every `N` finds.

- `--results-dir PATH`
  Choose a different output folder.

- `--resume PATH`
  Resume from a previously written checkpoint JSON.

- `--stop-file PATH`
  If this file exists, the script stops after the current completed job and writes a fresh checkpoint.

- `--checkpoint-every N`
  Write checkpoint progress every `N` completed jobs.

## Pause and Resume

This script supports checkpoint-based pause/resume.

Important detail:

- pause happens after the current scaffold job finishes
- it does not currently resume from the middle of an active DFS branch

That still allows you to stop the run, release CPU and RAM, and continue later without redoing completed scaffold jobs.

### Start a resumable run

From `cmd`:

```cmd
python scaffold_rectangle_search.py --max-scaffolds 3 --limit 0 --processes 19 --quiet-solutions --stop-file results\pause.flag
```

### Request a pause

From another `cmd` window in the same folder:

```cmd
type nul > results\pause.flag
```

The script will finish the current job, write the checkpoint, and exit.

### Resume later

```cmd
python scaffold_rectangle_search.py --resume results\scaffold_search_YYYYMMDD_HHMMSS_checkpoint.json
```

Before resuming, remove the pause flag if it still exists:

```cmd
del results\pause.flag
```

## Resume Overrides

When resuming, checkpoint values act as defaults, but explicit CLI options can override them.

Useful examples:

```cmd
python scaffold_rectangle_search.py --resume results\scaffold_search_..._checkpoint.json --progress-every 1
```

```cmd
python scaffold_rectangle_search.py --resume results\scaffold_search_..._checkpoint.json --processes 19 --print-solutions
```

## Example Commands

Single scaffold, all row positions:

```cmd
python scaffold_rectangle_search.py --max-scaffolds 1 --limit 5 --progress-every 1
```

Wider search using multiple processes:

```cmd
python scaffold_rectangle_search.py --max-scaffolds 3 --limit 0 --progress-every 1 --processes 19 --quiet-solutions
```

## Notes

- Different scaffold jobs have very different runtimes. A few hard jobs can dominate the wall time.
- Multiprocessing is used across scaffold jobs, not inside one job’s DFS.
- A resumed run can override compatible settings such as progress frequency, process count, stop-file path, and solution printing.
