# `scrabble_stack_search.py`

## Purpose

This script searches for stacked Scrabble grids built from full 7-letter horizontal plays.

The model is:

- each turn plays a full 7-letter row
- each new row is added directly to the current `top` or `bottom`
- every vertical word formed on that turn must already be valid
- the target height is configurable with `--rows`

So a run with `--rows 5` looks for a valid `5 x 7` stack, while still using 7-letter horizontal words.

## Data Source

By default the script reads:

`C:\Users\james\ScrabLab\resources\CSW24.db`

It expects a `words` table containing the lexicon.

## Search Model

The script uses depth-first search.

At any partial stack:

- the current vertical strings are tracked column by column
- the script asks which letters can legally extend each column at the front or back
- those per-column letter sets define the allowed letters for the next 7-letter row
- only 7-letter words that fit all 7 position constraints are tried
- if a branch cannot be extended, the search backtracks

This means it is not placing arbitrary rows and then checking legality afterwards. It only generates rows that already keep the vertical words valid at the moment they are played.

## Output

Solutions are printed as boxed grids unless `--quiet-solutions` is used.

Results are also saved automatically into the `results` folder as:

- `.txt` for readable grids and play order
- `.jsonl` for one structured solution per line
- `_summary.json` for run metadata and totals

## Main Arguments

- `--rows N`
  Search for a stack of `N` rows. Valid values are `2` to `7`.

- `--limit N`
  Stop after `N` solutions. Use `0` for no limit.

- `--row-limit N`
  Only load the top `N` candidate 7-letter row words.

- `--min-playability N`
  Ignore 7-letter row words below this playability.

- `--max-start-words N`
  Only try the first `N` starting rows.

- `--processes N`
  Number of CPU processes to use.

- `--progress-every N`
  Print progress every `N` starting rows.

- `--report-partial-depths`
  Print the first partial grid found at each new depth.

- `--quiet-solutions`
  Suppress full solution printing in the shell.

- `--append-every N`
  Flush saved solutions to disk every `N` finds.

- `--results-dir PATH`
  Choose a different output folder.

## Example Commands

Quick test:

```powershell
python scrabble_stack_search.py --rows 5 --row-limit 2000 --max-start-words 100 --report-partial-depths
```

Fuller run:

```powershell
python scrabble_stack_search.py --rows 5 --limit 0 --quiet-solutions --append-every 1000 --processes 4
```

## Notes

- `--rows` changes the number of stacked rows, not the row length.
- Horizontal plays are always 7 letters long.
- GPU is not used by this script.
- Repeated row words are allowed if the search finds them.
