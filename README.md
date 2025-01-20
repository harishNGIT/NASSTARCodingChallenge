# Wind Turbine Data Processing Pipeline

## Overview
This project implements a data processing pipeline for wind turbine data. It processes raw CSV files in raw folder, cleans the data, identifies anomalies, and computes statistics for further analysis. The pipeline also stores the results in a SQL Server database and handles incremental data loading efficiently.

---

## Features
1. **Incremental Data Processing**:
   - Only processes new data since the last load timestamp for each turbine group.

2. **Data Cleaning**:
   - Fills missing `power_output` values with the mean of the respective turbine group.
   - Removes outliers based on a configurable threshold 2

3. **Anomaly Detection**:
   - Flags anomalies using z-scores or standard deviation thresholds.
   - Configurable sensitivity using `ANOMALY_THRESHOLD` 3

4. **Statistics Computation**:
   - Computes daily minimum, maximum, and mean `power_output` values for each turbine.

5. **Database Integration**:
   - Uses SQLAlchemy with `pyodbc` to store cleaned data, statistics, and anomaly flags.
   - Maintains an audit table to track the last processed timestamp for each turbine group.

6. **Concurrency**:
   - Processes multiple files concurrently using Python’s `concurrent.futures` module.

7. **Logging**:
   - Detailed logs for debugging and monitoring stored in `pipeline.log` file.

---

## Configuration
### Database Connection
- The pipeline connects to a SQL Server database using the following configuration:

```python
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LAPTOP-MBV00UBE\\SQLEXPRESS;"
    "DATABASE=test;"
    "Trusted_Connection=yes;"
)

water mark table used for incrimental loading
CREATE TABLE [audit].[watermark](
	[lastLoadedTimeStamp] [datetime2](3) NULL,
	[groupName] [nvarchar](50) NULL,
	[UpdatedTimeStamp] [datetime2](3) NULL
) ON [PRIMARY]
GO

ALTER TABLE [audit].[watermark] ADD  DEFAULT (getdate()) FOR [UpdatedTimeStamp]
GO

statastics data stored in turbine_statastics

CREATE TABLE [dbo].[turbine_statastics](
	[turbine_id] [bigint] NULL,
	[period] [datetime] NULL,
	[min_power_output] [float] NULL,
	[max_power_output] [float] NULL,
	[mean_power_output] [float] NULL,
	[UpdatedTimestamp] [datetime] default getDate()
) ON [PRIMARY]
GO


turbine data stored in turbine_data

CREATE TABLE [dbo].[turbine_data](
	[timestamp] [datetime] NULL,
	[turbine_id] [bigint] NULL,
	[wind_speed] [float] NULL,
	[wind_direction] [bigint] NULL,
	[power_output] [float] NULL,
	[is_anomaly] [bit] NULL,
	[UpdatedTimestamp] [datetime] default getDate()
) ON [PRIMARY]
GO

```
- Modify the connection string to point to your database server and instance.

### Thresholds
- `OUTLIER_THRESHOLD`: Defines the number of standard deviations to filter outliers.
- `ANOMALY_THRESHOLD`: Defines the number of standard deviations to flag anomalies.

---

## Directory Structure
```plaintext
project/
├── raw/                    # Directory containing raw CSV files
├── pipeline.log            # Log file for the pipeline
├── main.py                 # Main script
├── HelperFunctions/        # Directory for helper modules
│   ├── __init__.py
│   ├── data_cleaning.py    # Functions for cleaning data
│   ├── db_utils.py         # Functions for database interactions
│   ├── anomaly_detection.py # Functions for detecting anomalies
├── tests/                  # Unit tests for the pipeline
│   ├── test_data_cleaning.py
│   ├── test_db_utils.py
│   ├── test_anomaly_detection.py
└── requirements.txt        # Python dependencies
```

---

## Installation
### Prerequisites
- Python 3.8 or later
- SQL Server with necessary tables:
  - `audit.watermark`: Tracks last loaded timestamps.
  - `turbine_data`: Stores cleaned turbine data.
  - `turbine_statastics`: Stores computed statistics.

### Dependencies
Install the required Python libraries using pip:

```bash
pip install -r requirements.txt
```

---

## Usage
### 1. Prepare Raw Data
- Place raw CSV files in the `raw/` directory. Each file should follow this format:

```csv
timestamp,turbine_id,wind_speed,wind_direction,power_output
2022-03-01 00:00,1,11.8,169,2.7
2022-03-01 00:00,2,11.6,24,2.2
...
```

### 2. Run the Pipeline
Run the main script:

```bash
python main.py
```

### 3. Output
- **Database**: Cleaned data, statistics, and anomalies are stored in SQL Server.
- **Logs**: Detailed logs are available in `pipeline.log`.

---

## Testing
Run unit tests using `pytest`:

```bash
pytest tests/
```

---

## Key Functions
### `LoadAndcleanData(filepath, groupName)`
- Reads raw CSV files and cleans the data.
- Handles missing values and removes outliers.

### `IdentifyAnamolies(df, groupName)`
- Flags anomalies in `power_output` based on z-scores or thresholds.

### `ComputeStats(df, period, groupName)`
- Computes daily statistics (`min`, `max`, `mean`) for `power_output`.

### `updateLastLoadTime(groupName, timestamp)`
- Updates the `audit.watermark` table with the latest timestamp.

### `process_files_concurrent(file_paths, turbineMappingDic, max_workers=None)`
- Processes multiple files concurrently.

---

## Contributing
1. Fork the repository.
2. Create a new branch for your feature (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m 'Add some feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

---

## License
This project is licensed under the MIT License. See the LICENSE file for details.

---

## Acknowledgments
- Python Community
- SQLAlchemy and Pandas Documentation


