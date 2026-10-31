# Project: Real-Time Financial Data Pipeline (ETL)

## Tech Stack Mandates
- **Language:** Python 3.x
- **Data Transformation:** Pandas
- **Storage:** SQLite
- **Environment:** Use a virtual environment (recommended)

## Architectural Patterns
- **Modularity:** Separate data generation, ETL logic, and storage.
- **Robustness:** ETL must handle messy data (missing values, duplicates, format inconsistencies).
- **Time-Series Focus:** Aggregations should be time-windowed (e.g., per minute).

## Directory Structure
- `data_generator/`: Scripts for simulating raw financial streams.
- `pipeline/`: Core ETL logic and transformation scripts.
- `database/`: SQLite database files and schema definitions.
