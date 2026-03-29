# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

_Table DDL or DataFrame schema. Explain what is stored and why it is kept as-is._

### Silver

_Table DDL or DataFrame schema. Explain what changed compared to bronze and why._

---

### Changes from Bronze

**1. Parsing (Raw → Structured)**
- Extracted JSON from `value` column into structured fields  
- Bronze: raw Kafka format (`key`, `value`, etc.)  
- Silver: usable columns  

**2. Type Casting**
- Converted fields to proper types (`TIMESTAMP`, `INT`, `DOUBLE`)  

**3. Data Cleaning**
- Removed rows with:
  - null timestamps or location IDs  
  - invalid duration (`dropoff <= pickup`)  
  - non-positive distance  
  - negative fare  
- Replaced `passenger_count` NULL/≤0 → `1`  
- Removed duplicates  

**4. Enrichment**
- Joined with zone lookup table:
  - `PULocationID` → pickup info  
  - `DOLocationID` → dropoff info  
- Added borough, zone, service zone  

**5. Integrity Filtering**
- Removed rows with missing zone matches  

### Gold

_Table DDL or DataFrame schema. Explain the aggregation logic._

## 2. Cleaning rules and enrichment

_List each cleaning rule (nulls, invalid values, deduplication key) with a brief justification._
_Describe the enrichment step (zone lookup join)._
### Cleaning Rules

- **Null handling**
  - Dropped rows with null values in:
    - `tpep_pickup_datetime`, `tpep_dropoff_datetime`
    - `PULocationID`, `DOLocationID`
  - *Justification:* These fields are required to define a valid trip

- **Passenger count normalization**
  - Replaced `NULL` or `≤ 0` with `1`
  - *Justification:* Real-world data contains invalid/missing values; prevents unnecessary data loss

- **Valid trip duration**
  - Kept only rows where `dropoff > pickup`
  - *Justification:* Ensures logical consistency of trips

- **Trip distance filter**
  - Removed rows with `trip_distance ≤ 0`
  - *Justification:* Non-positive distances are invalid

- **Fare validation**
  - Removed rows with `fare_amount < 0`
  - *Justification:* Negative fares are not realistic

- **Deduplication**
  - Applied `dropDuplicates()` on all columns
  - *Justification:* Prevents duplicate records from ingestion/streaming

---

### Enrichment

- Joined with `taxi_zone_lookup` dataset using:
  - `PULocationID → pickup zone`
  - `DOLocationID → dropoff zone`

- Added:
  - `pickup_borough`, `pickup_zone`, `pickup_service_zone`
  - `dropoff_borough`, `dropoff_zone`, `dropoff_service_zone`

- Removed rows where lookup failed (null zones)

- *Purpose:*  
  Convert location IDs into human-readable geographic information for analysis

## 3. Streaming configuration

_Describe:_
- _Checkpoint path and what it stores._
- _Trigger interval and why you chose it._
- _Output mode (append/update/complete) and why._
- _Watermark (if used) and why._

## 4. Gold table partitioning strategy

_Explain your partitioning choice. Why this column(s)? What query patterns does it optimize?_
_Show the Iceberg snapshot history (query output or screenshot)._

## 5. Restart proof

_Show that stopping and restarting the pipeline does not produce duplicates._
_Include row counts before and after restart._

## 6. Custom scenario

_Explain and/or show how you solved the custom scenario from the GitHub issue._

## 7. How to run

```bash
# Step 1: Start infrastructure
docker compose up -d

# Step 2: Start the producer
python produce.py

# Step 3: Run the pipeline
<your command here>
```

_Add any additional steps or dependencies needed to reproduce your results._

_Include the `.env` values the grader should use to run your project._