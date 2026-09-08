# Lab 3 — MapReduce and Spark DataFrames

This repository contains the complete implementation, reproducible Docker
environment, final output files, report source, and validation evidence for all
four Lab 3 tasks.

## Current submission status

- Code builds and runs successfully in Docker.
- All four outputs were regenerated on 2026-09-08.
- Task 1 Java reference validation: zero mismatches.
- Task 2-1 independent Pandas recomputation: zero mismatches.
- Task 2-2 independent NumPy type-7 validation: zero mismatches across 32,972
  exact group-percentile rows.
- Report chapters in `docs/tex_prism_chapters/` are corrected and synchronized
  with the final benchmark evidence; their README records the Prism import order
  and image dependencies.
- `docs/Report.pdf` is intentionally absent until the corrected chapters are
  exported again from Prism/Overleaf. The previous PDF is preserved in
  `docs/archive/` and must not be submitted.
- `docs/drive_link.txt` still needs the group's real shared Drive URL.

## Repository layout

```text
Lab3/
├── data/
│   └── raw/                      # Immutable input dataset
│
├── src/                          # Scala implementation
│   ├── shared/                   # Parsing, business rules, output utilities
│   ├── Task_1-1/                 # MapReduce sliding window
│   ├── Task_1-2/                 # MapReduce median variety
│   ├── Task_2-1/                 # Spark joins and plan analysis
│   └── Task_2-2/                 # Spark percentile comparison
│
├── scripts/                      # Benchmarks, profiling, validators
│
├── output/                       # Generated results
│   ├── Task_1-1.csv              # Required deliverable
│   ├── Task_1-2.csv              # Required deliverable
│   ├── Task_2-1.parquet          # Required deliverable
│   └── Task_2-2.parquet          # Required deliverable
│
├── docs/                         # Report and supporting documentation
│   ├── tex_prism_chapters/       # Authoritative corrected report source
│   ├── evidence/                 # Benchmarks, plans, validations, hashes
│   ├── review/                   # Teacher-style review and team checklist
│   ├── archive/                  # Superseded draft and old PDF
│   ├── data_rules.md             # Canonical data/business contract
│   ├── PREPARATION.md            # Environment runbook
│   └── README.md                 # Documentation index
│
├── specs/                        # Assignment and reference slides
├── docker-compose.yml            # Hadoop/YARN/Spark teaching cluster
├── pom.xml                       # Maven/Scala build configuration
├── setup-lab3.ps1                # Start and verify the services
└── verify-preparation.ps1         # Build and run smoke checks
```

## Quick start

Start Docker Desktop, then run from the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\setup-lab3.ps1 -SkipPull
powershell -NoProfile -ExecutionPolicy Bypass -File .\verify-preparation.ps1 -SkipSetup
```

Run the repeatable benchmarks and independent output validation:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\benchmark-task1.ps1 -Runs 5 -Pipeline all
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\benchmark-task21.ps1 -Runs 5
python .\scripts\validate-final-outputs.py
```

Task-specific commands and output schemas are documented in each
`src/Task_*/source/README.md`. The complete evidence index is in
`docs/evidence/README.md`.

## Required deliverables

Upload only these four generated answer files to the shared Drive location:

```text
output/Task_1-1.csv
output/Task_1-2.csv
output/Task_2-1.parquet
output/Task_2-2.parquet
```

Sensitivity files, `.crc` sidecars, smoke directories, temporary work
directories, the archived report, and evidence files are supporting material;
they do not replace the four required outputs.

## Final manual checklist

1. Export the corrected report chapters to `docs/Report.pdf` and inspect every
   table, equation, and Task 2-1 screenshot.
2. Create `docs/drive_link.txt` containing one accessible Drive URL.
3. Confirm the Drive folder contains the exact four filenames above.
4. Package the required source/report structure under one representative-ID
   root folder and test the ZIP from a clean directory.
