# Real Estate Data Pipeline — Setup Guide

## 1. Requirements

- Python 3.11 or higher
- pip (comes with Python)

---

## 2. Folder Structure

Create the following structure on your computer. The pipeline will auto-create
the `output/` and `merge/` subfolders on first run, but you need to create the
top-level project folder and place the files manually.

```
realestate_pipeline/
│
├── main.py                        ← Run this to start the pipeline
├── config.py                      ← All settings and constants
├── requirements.txt               ← Python dependencies
│
├── steps/
│   ├── __init__.py
│   ├── step1_clean.py
│   ├── step2a_merge.py
│   ├── step2b_split.py
│   ├── step2c_tagcheck.py
│   ├── step2d_zestimate.py
│   ├── step2e_dataflick.py
│   ├── step2f_canadian.py
│   ├── step2g_namesplit.py
│   ├── step2h_buybox.py
│   ├── step3_audit.py
│   └── step4_skiptrace.py
│
├── utils/
│   ├── __init__.py
│   ├── file_helpers.py
│   └── name_helpers.py
│
├── input/                         ← DROP YOUR RAW FILES HERE
│
├── output/                        ← Auto-created on first run
│   ├── step1_clean/
│   ├── step2_optional/
│   ├── step3_audit/
│   ├── step4_skiptrace/
│   │   └── export/
│   └── zestimate/
│       ├── export/
│       └── merged/
│
└── merge/                         ← Auto-created on first run
    ├── zestimate/                 ← DROP WSE RESULTS HERE
    └── skiptrace/                 ← DROP SKIPTRACE RESULTS HERE (future)
```

---

## 3. Installation — Step by Step

### Step 1 — Install Python
Download and install Python 3.11+ from https://python.org/downloads

During installation on Windows, check **"Add Python to PATH"**.

Verify installation:
```bash
python --version
```

---

### Step 2 — Download or copy the project files
Place all project files into a folder on your computer, for example:
```
C:\Users\YourName\Documents\realestate_pipeline\
```

---

### Step 3 — Open a terminal in the project folder

**Windows:**
1. Open File Explorer and navigate to the project folder
2. Click the address bar, type `cmd`, press Enter

**Mac / Linux:**
1. Right-click the project folder
2. Select "Open Terminal here"

---

### Step 4 — Install dependencies
Run this command in the terminal:
```bash
pip install -r requirements.txt
```

Wait for all packages to install. You should see a success message at the end.

---

### Step 5 — Place your input files
Copy your raw Excel files (`.xlsx`) into the `input/` folder:

```
realestate_pipeline/
└── input/
    ├── Miami 5K Direct Mail.xlsx
    ├── Orlando 3K Cold Calling.xlsx
    └── Tampa 2K SMS.xlsx
```

---

### Step 6 — Run the pipeline
```bash
python main.py
```

The CLI will guide you through each step interactively.

---

## 4. How the Pipeline Works

```
INPUT FILES (input/)
       ↓
[Step 1] Clean              → output/step1_clean/
       ↓
[Step 2] Optional           → output/step2_optional/
   a. Merge
   b. Split
   c. Tag Check
   d. Zestimate Tool ──────→ output/zestimate/export/   (upload to WSE)
   e. Dataflick Format         merge/zestimate/          (drop results here)
   f. Canadian Filter          output/zestimate/merged/  (merged output)
   g. Name Splitter
   h. BuyBox HQ
       ↓
[Step 3] Audit              → terminal output only
       ↓
[Step 4] Skiptrace          → output/step4_skiptrace/export/
```

Each optional step reads from the previous step's output (chained). If a step
produces no output, the next step falls back to the most recent folder that
has files.

---

## 5. Step 2 Chaining Logic

| Scenario | Next step reads from |
|---|---|
| No optional steps run | `step1_clean/` |
| Only Merge (2a) run | `step2_optional/` |
| Merge then Split run | Split reads Merge output in `step2_optional/` |
| Any Step 2 run | Step 3 reads from `step2_optional/` |

---

## 6. Zestimate Workflow

The Zestimate tool runs in two separate passes because the WSE provider has
no direct API trigger from this pipeline.

**First pass — Export:**
1. Run the pipeline and select Step 2D → option `1`
2. Upload the generated file from `output/zestimate/export/` to your WSE provider
3. Wait for the job to complete and receive your Job ID

**Second pass — Merge:**
1. Run the pipeline and select Step 2D → option `2`
2. Enter your WSE API Key and Job ID when prompted
3. Select the original input file to merge against
4. Find the merged output in `output/zestimate/merged/`

> The WSE API Key is entered at runtime each time — it is not stored anywhere.

---

## 7. Skiptrace Workflow

**Export:**
1. Run the pipeline and proceed to Step 4
2. Select the cadence (DM, CC, SMS)
3. Upload the generated file from `output/step4_skiptrace/export/` to your provider
4. Wait for results

**After results come back:**
1. Drop the results file into `merge/skiptrace/`
2. Post-merge step can be added in a future update

---

## 8. Clean Step — What Gets Rejected

The clean step removes rows and logs them in `output/step1_clean/Rejected_Properties.xlsx`:

| Rejection Stage | Condition |
|---|---|
| Empty Action Plans | `ACTION PLANS` column is blank |
| Duplicate Address | Same `MAILING ADDRESS` + `MAILING ZIP` |
| Duplicate Owner | Same `OWNER FULL NAME` + `ADDRESS` + `ZIP` |
| Empty Owner Full Name | `OWNER FULL NAME` is blank or null |
| Unwanted Names | Name contains keywords like Bank, Church, County, etc. |
| Unwanted Owner Type | Owner type is Non Sellers or Religious Organization |

---

## 9. File Naming Conventions

For the Merge and Split steps to correctly identify files by cadence, your
input files should contain the cadence keyword in their filename:

| Cadence | Filename must contain |
|---|---|
| Direct Mail | `Direct Mail` |
| Cold Calling | `Cold Calling` |
| SMS | `SMS` |

Examples:
- `Miami 5K Direct Mail.xlsx` ✓
- `Orlando_CC_3200.xlsx` ✗ (use `Cold Calling` not `CC` in filename)
- `Tampa SMS 2K.xlsx` ✓

---

## 10. Troubleshooting

**"No Excel files found"**
→ Make sure your files are in the `input/` folder and are `.xlsx` format

**"Missing required columns"**
→ The Merge step validates column names strictly. See `config.py` →
`REQUIRED_COLUMNS` for the full list per cadence.

**pip install fails**
→ Try `pip3 install -r requirements.txt` or
`python -m pip install -r requirements.txt`

**Permission errors on Windows**
→ Run the terminal as Administrator, or make sure no output files are
currently open in Excel
