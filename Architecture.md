# Architecture Documentation — BusinessDataChecker

## Overview

**BusinessDataChecker** is a Maven multi-module project for building and executing business validation checks on Apache Spark DataFrames. The system compares two datasets (left and right) and runs configurable checks to detect discrepancies in row counts, column values, and column data types.

The project follows a **Strategy + Factory pattern** architecture, enabling extensible check types and clean separation of concerns.

---

## Technology Stack

| Component        | Technology                   |
|------------------|------------------------------|
| Build System     | Maven 3.x                    |
| JVM Language     | Scala 2.12.11, Java 1.8     |
| Data Processing  | Apache Spark SQL 2.4.5      |
| Join Utilities   | Nageltrekker 0.1.3 (external library, jitpack.io) |

---

## Project Module Structure

```
businessdatachecker (pom - parent)
 └── joinatool (jar)
      ├── src/main/java/com/boro/apps/joinatool/
      │   ├── CheckStatus.java        — Enum: NEW, ACTIVE, EXPIRED
      │   └── Codes.java              — Enum: COUNT_ROWS, EQUAL_ON_VAL, TYPES_EVAL
      └── src/main/scala/com/boro/apps/joinatool/
          ├── SparkMain.scala         — Entry point; initialises SparkSession & test data
          ├── checkimpl/
          │   ├── CheckHolder.scala           — Trait (strategy interface)
          │   ├── CheckCountRows.scala        — Row count equality check
          │   ├── CheckEqualOnVal.scala       — Column value equality check
          │   └── CheckTypesEval.scala        — Column type consistency check
          ├── checkservice/
          │   └── CheckService.scala          — Orchestrator for check execution
          ├── dfservice/
          │   └── DfService.scala             — DataFrame join abstraction
          ├── domain/
          │   └── DfAggregation.scala         — Domain case classes
          └── factory/
              └── CheckServiceFactory.scala   — Factory mapping Codes→CheckHolder
```

---

## Architecture Diagram (PlantUML)

```plantuml
@startuml BusinessDataChecker Architecture

skinparam packageStyle rectangle
skinparam backgroundColor #FEFEFE

' ─── Boundary: External Systems ───
package "External" {
  [Apache Spark Session] as Spark
  [Nageltrekker Library] as Nageltrekker
}

' ─── Boundary: Domain Layer ───
package "Domain Layer (domain)" {
  class DfAggregation {
    + tableName: String
    + dfLeft: DataFrame
    + dfRight: DataFrame
    + seqColumns: Seq[String]
  }
  class TableResult {
    + tableName: String
    + dfResult: DataFrame
  }
  class TableResultWithPrepDF {
    + tableName: String
    + dfLeft: DataFrame
    + dfRight: DataFrame
    + dfResult: DataFrame
  }
  class CheckStatistics {
    + tableName: String
    + mapCheckStatistics: Map[_, _]
  }
  class TableStatistics {
    + tableName: String
    + dfWithStatistics: DataFrame
  }
  class Check {
    + code: Codes
    + stat: Any
    + status: CheckStatus
    + result: Result
  }
  class Result {
    + res: Boolean
    + tmstp: Timestamp
  }
}

' ─── Boundary: Enums ───
package "Enums" {
  enum Codes {
    COUNT_ROWS
    EQUAL_ON_VAL
    TYPES_EVAL
  }
  enum CheckStatus {
    NEW
    ACTIVE
    EXPIRED
  }
}

' ─── Boundary: Service Layer ───
package "Service Layer" {
  class DfService {
    + joinResult(): TableResult
    + joinResultWithPrepDF(): TableResultWithPrepDF
  }
  class CheckService {
    + getCheckDetails(code: Codes): Map[String, Check]
    + getCheckDF(code: Codes): TableStatistics
  }
}

' ─── Boundary: Check Implementations (Strategy) ───
package "Check Implementations (checkimpl)" {
  interface CheckHolder {
    + getCalculationMap(): CheckStatistics
    + getCalculationDf(): TableStatistics
    + getCheckBool(map): Boolean
  }
  class CheckCountRows
  class CheckEqualOnVal
  class CheckTypesEval
}

' ─── Boundary: Factory ───
package "Factory" {
  class CheckServiceFactory {
    + getCheckImpl(codes: Codes): CheckHolder
  }
}

' ─── Boundary: Entry Point ───
package "Entry Point" {
  class SparkMain {
    + spark: SparkSession
    + main(args: Array[String]): Unit
  }
}

' ─── Relationships ───
DfService --> DfAggregation : uses
DfService --> Nageltrekker : prepareDf()
DfService --> TableResult : produces
DfService --> TableResultWithPrepDF : produces

CheckService --> CheckServiceFactory : getCheckImpl()
CheckService --> DfService : uses
CheckService --> CheckHolder : delegates to
CheckService --> CheckStatistics : receives from strategy
CheckService --> Check : wraps result

CheckCountRows ..|> CheckHolder : implements
CheckEqualOnVal ..|> CheckHolder : implements
CheckTypesEval ..|> CheckHolder : implements

CheckCountRows --> DfService : calls joinResultWithPrepDF()
CheckEqualOnVal --> DfService : calls joinResult()
CheckTypesEval --> DfService : calls joinResult()
CheckEqualOnVal --> Nageltrekker : checkEqualColumns()
CheckTypesEval --> Nageltrekker : checkEqualColumnTypes()

CheckServiceFactory --> Codes : maps from
CheckServiceFactory --> CheckCountRows : instantiates
CheckServiceFactory --> CheckEqualOnVal : instantiates
CheckServiceFactory --> CheckTypesEval : instantiates

SparkMain --> Spark : creates session
SparkMain --> CheckService : instantiates
SparkMain --> DfService : instantiates
SparkMain --> DfAggregation : creates config

Check --> Codes : references
Check --> CheckStatus : references
Check --> Result : contains

@enduml
```

---

## Component Descriptions

### 1. Entry Point — `SparkMain`
- Initializes a local `SparkSession` with `local[6]` master.
- On first run, creates three Hive-style tables (`table1`, `table2`, `table3`) with synthetic data.
- Instantiates `CheckService` with a `DfService` and runs all three check types against two comparison pairs:
  - `table1` vs `table2` (deliberately different values — checks should fail)
  - `table1` vs `table3` (mostly identical — checks should pass)
- Prints both textual check details and full DataFrames with statistics.

### 2. Service Layer

#### `DfService`
- Accepts a `DfAggregation` (two DFs plus join keys).
- Provides two methods:
  - `joinResult` — returns merged DF using external `prepareDf()` (from Nageltrekker).
  - `joinResultWithPrepDF` — same but also exposes original left/right DFs (needed by `CheckCountRows`).

#### `CheckService`
- Orchestrates a single check:
  1. Retrieves the correct `CheckHolder` implementation from `CheckServiceFactory`.
  2. Calls `getCalculationMap` / `getCalculationDf` to compute statistics.
  3. Evaluates the boolean result via `getCheckBool`.
  4. Wraps everything into a `Check` object with `CheckStatus.NEW` and a timestamp.

### 3. Check Implementations (Strategy Pattern)

All implementations extend the `CheckHolder` trait:

| Implementation       | `getCalculationMap` return type         | `getCalculationDf` return type    | `getCheckBool` logic                                  |
|----------------------|-----------------------------------------|-----------------------------------|-------------------------------------------------------|
| **CheckCountRows**   | `Map[String, Long]` — counts of dfLeft, dfRight, dfResult | Empty DataFrame         | All three counts are equal                            |
| **CheckEqualOnVal**  | `Map[String, (Long, Long)]` — per column (match count, match %) | DataFrame with comparison details | Every column's match % equals 100                     |
| **CheckTypesEval**   | `Map[String, (String, String)]` — per column (left type, right type) | DataFrame with type comparison | Left type equals right type for every column          |

### 4. Factory — `CheckServiceFactory`
- Maps each `Codes` enum value to its corresponding `CheckHolder` implementation.
- Throws `UnsupportedOperationException` for unknown codes.

### 5. Domain Layer
- **`DfAggregation`**: Input configuration — table name, left DF, right DF, join key columns.
- **`TableResult` / `TableResultWithPrepDF`**: Intermediate results after joining.
- **`CheckStatistics` / `TableStatistics`**: Output containers for check results (map-based or DataFrame-based).
- **`Check`**: Complete check record containing the code, statistics, status, and boolean result with timestamp.
- **`Result`**: Wraps boolean pass/fail with a timestamp.
- **`Calculation` / `ResultSet`**: Additional domain classes for future use (present but not actively used in v1.0).

### 6. Enums (Java)
- **`Codes`**: Enum identifying check type — `COUNT_ROWS`, `EQUAL_ON_VAL`, `TYPES_EVAL`.
- **`CheckStatus`**: Enum for check lifecycle — `NEW`, `ACTIVE`, `EXPIRED`.

---

## Data Flow

```
                         ┌─────────────────────┐
                         │   SparkMain          │
                         │  (Entry Point)       │
                         └──────────┬──────────┘
                                    │
                   ┌────────────────┼────────────────┐
                   │                │                 │
                   ▼                ▼                 ▼
           DfAggregation     DfService         CheckService
         (tableName, df,  ──►  joinResult /  ──►  getCheckDetails
          joinKeys)              joinResultWith    getCheckDF
                                 PrepDF                │
                                      │                │
                                      ▼                ▼
                              Nageltrekker     CheckServiceFactory
                              (prepareDf())          │
                                                      │
                                                      ▼
                                              CheckHolder (strategy)
                                                │
                                          ┌─────┼─────┐
                                          ▼     ▼     ▼
                                   CountRows  EqualOnVal  TypesEval
                                          │     │     │
                                          ▼     ▼     ▼
                                      CheckStatistics / TableStatistics
                                          │
                                          ▼
                                      Check (code, stat, status, result)
```

---

## Extensibility

To add a new check type:

1. Add a new value to the `Codes` enum.
2. Create a new class extending `CheckHolder` in `checkimpl/`.
3. Implement the three abstract methods.
4. Register the mapping in `CheckServiceFactory`'s `getCheckImpl` match block.

No changes are required in `CheckService` or `SparkMain` — the new check is automatically available.

---

## Dependencies

| Dependency        | Version | Scope     | Source             |
|-------------------|---------|-----------|--------------------|
| Apache Spark SQL  | 2.4.5   | compile   | Maven Central      |
| Scala Library     | 2.12.11 | compile   | Maven Central      |
| Nageltrekker      | 0.1.3   | compile   | jitpack.io (GitHub)|

The **Nageltrekker** library (by the same author) provides the core DataFrame comparison utilities:
- `prepareDf(dfLeft, dfRight, joinCols)` — joins two DataFrames on specified columns with renamed columns.
- `checkEqualColumns(joinedDf)` — compares values per column, returns match counts and percentages.
- `checkEqualColumnTypes(spark, joinedDf)` — compares column data types between left and right sides.