# ddcrate

An implementation of the 2018+ WFDF [Double Disc Court](https://wfdf.sport/disciplines/overall/ddc/) ranking system specified at <https://wfdf.sport/world-rankings/double-disc-court-ddc/> .

## Tournament result format

`ddcrate` contains utilities for reading tournament results from TSV files.
`ddcrate-cli` wraps these in a CLI.

Results reside in a directory.
Within that directory are subdirectories representing levels of tournament: `small`, `medium`, `major`, and `championship`.
These may contain arbitrary file hierarchies (for example, they could be split by region, or by time period).
Result files are TSVs whose names start with an ISO-8601 date and end with `.tsv`.

These TSVs contain teams' finishing positions:
each row contains the finishing position and the two integer player IDs, separated by tabs.
For example:

```tsv
1       235476  529052
2       23342   4235211978
2       234871  1387235
4       5690845 5638906
```

Lines with insufficent fields (including empty lines) are ignored.
Additional fields after the first 3 are allowed, and ignored.
Lines starting with `#` are ignored.
Records do not have to be in ranking order.

Note the handling of ties: multiple teams can have the same finishing position,
but the next team below the tie must be ranked as if the teams above each had their own position.
