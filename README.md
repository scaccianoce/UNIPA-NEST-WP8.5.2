# LCAUDBS

LCAUDBS is a Python-based open-source workflow for generating EnergyPlus models from GIS building footprints, simulating energy performance at cluster scale, evaluating early-stage LCA indicators, and exploring discrete retrofit combinations through Pareto optimization. The present work has been developed within the project funded under the National Recovery and Resilience Plan (NRRP), Mission 4 Component 2 Investment 1.3 – Call for tender N. 341 of 15.03.2022 of Ministero dell'Università e della Ricerca (MUR); funded by the European Union – NextGenerationEU. Award Number: Project code PE0000021, Concession Decree No. 1561 of 11.10.2022 adopted by Ministero dell’Università e della Ricerca (MUR), CUP E63C22002160007, Project title "Network 4 Energy Sustainable Transition – NEST".

The tool currently supports:

- geometry-driven IDF generation starting from a shapefile of building polygons;
- assignment of construction typologies and operational schedules from Excel workbooks;
- EnergyPlus execution for multiple buildings and weather files;
- early-stage LCA evaluation using a compact climate-change database;
- discrete multi-scenario optimization based on the comparison between `Tipologie_Base` and `Tipologie_Intervento`;
- export of scenario results, Pareto points, intervention summaries, and technical reports.

## Repository contents

- `main.py`: main application entry point.
- `Template_input_ottimizzazione.xlsx`: default external template for optimization inputs.
- `docs/`: engineering manual and LCA report in Markdown, LaTeX, Word, and generated PDF.
- `requirements.txt`: Python dependencies.

## Main input files

The workflow relies on the following main inputs:

1. A shapefile containing the building footprints of the district or urban cluster.
2. An IDF base template (`filebase.idf`) used as the starting EnergyPlus model.
3. One or more weather files in `.epw` format.
4. An Excel workbook for typologies.

Two typology approaches are supported:

- legacy workbook mode using `Tipologie_edilizie_misure.xlsx`;
- template-driven mode using `Template_input_ottimizzazione.xlsx`.

The optimization workflow expects the template workbook and uses, in particular, these sheets:

- `Tipologie_Base`
- `Tipologie_Intervento`
- `Variabili_Ottimizzazione`
- `Mapping_EnergyPlus_LCA`
- `Early_LCA_ClimateChange`
- `Parametri_Analisi`

## Python requirements

Install the Python packages listed in `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## External software requirements

This repository also requires external software that is not installed with `pip`:

- `EnergyPlus`: required to run the building simulations.
- `ReadVarsESO`: usually distributed with EnergyPlus and used to extract tabular outputs.
- A Python installation that includes `tkinter` if you want to use the GUI mode.
- An optional LaTeX distribution if you want to compile the `.tex` reports manually.

The code already contains default EnergyPlus installation paths for macOS and Windows, but they can be overridden through the CLI.

## How to run the tool

### 1. GUI mode

```bash
python3 main.py --gui
```

This opens the graphical interface, from which you can select the Excel/template input, shapefile, EnergyPlus folder, output folder, and optimization mode.

### 2. CLI mode with default paths

```bash
python3 main.py --run-defaults
```

This runs the workflow using the default paths encoded in the application.

### 3. CLI mode with optimization

```bash
python3 main.py --run-defaults --optimize
```

### 4. CLI mode with explicit template and output directory

```bash
python3 main.py \
  --input-template Template_input_ottimizzazione.xlsx \
  --typology-sheet Tipologie_Intervento \
  --shapefile shapefile/Cuba-Calatafimi_District.shp \
  --idf-template filebase.idf \
  --weather-dir ITA_Palermo \
  --energyplus-dir /Applications/EnergyPlus-24-2-0 \
  --output-dir output_run \
  --optimize
```

## Command-line options

The main CLI options are:

- `--gui`: starts the graphical interface;
- `--run-defaults`: runs the workflow with default repository paths;
- `--excel`: path to the legacy typology Excel workbook;
- `--input-template`: path to the optimization template workbook;
- `--typology-sheet`: sheet name to read from the template;
- `--optimize`: runs discrete multi-scenario optimization and Pareto extraction;
- `--shapefile`: path to the input shapefile;
- `--idf-template`: path to the base IDF file;
- `--weather`: one or more `.epw` weather files;
- `--weather-dir`: directory from which all `.epw` files are collected automatically;
- `--energyplus-dir`: EnergyPlus installation directory;
- `--output-dir`: output directory;
- `--start-index` and `--end-index`: process only a subset of buildings;
- `--skip-reports`: skips final report and merged DXF generation.

You can also inspect the live help at any time:

```bash
python3 main.py --help
```

## Outputs

The tool can generate:

- EnergyPlus input models for each building and scenario;
- EnergyPlus simulation folders;
- tabular summaries such as `outputtbl.csv` and `output.xlsx`;
- optimization summaries such as:
  - `optimization_results.csv`
  - `optimization_results.xlsx`
  - `optimization_interventions_by_typology.csv`
  - `pareto_points.csv`
- a Pareto chart embedded in the Excel optimization workbook;
- technical documentation in `docs/`.

## Typical workflow

1. Prepare or update the shapefile, weather files, and the Excel template.
2. Install the Python dependencies with `pip install -r requirements.txt`.
3. Install EnergyPlus and verify the `energyplus` directory path.
4. Run a baseline simulation.
5. Run the optimization with `--optimize`.
6. Inspect `optimization_results.xlsx` and `pareto_points.csv`.
7. Review the generated technical documentation in `docs/`.

## Notes on the current optimization approach

The current implementation is discrete rather than continuous.

This means that:

- scenarios are generated from intervention combinations and workbook differences;
- the Pareto front is extracted from explicitly simulated scenarios;
- continuous automatic generation of insulation thickness variants in EnergyPlus is not yet implemented.


## Documentation

The repository already contains:

- `docs/Engineering_Reference_Manual.md`
- `docs/Engineering_Reference_Manual.tex`
- `docs/Engineering_Reference_Manual.docx`
- `docs/LCA_Early_Database_Report.md`
- `docs/LCA_Early_Database_Report.tex`
- `docs/LCA_Early_Database_Report.docx`

These files document the architecture, mathematical model, LCA database logic, and the implemented optimization workflow.
