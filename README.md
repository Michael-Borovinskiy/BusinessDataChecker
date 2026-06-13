# BusinessDataChecker

**Costruction of business checks mechanism with using sources of Spark DF**

A Maven multi-module project for building and executing configurable business validation checks on Apache Spark DataFrames. The system compares two datasets (left and right) using join keys and runs pluggable checks to detect discrepancies in row counts, column values, and column data types.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run](#run)
- [How It Works](#how-it-works)
  - [Check Types](#check-types)
  - [Data Flow](#data-flow)
- [Module: `joinatool`](#module-joinatool)
  - [Entry Point](#entry-point)
  - [Service Layer](#service-layer)
  - [Check Implementations](#check-implementations)
  - [Factory](#factory)
  - [Domain Model](#domain-model)
- [Extending with New Checks](#extending-with-new-checks)
- [Dependencies](#dependencies)
- [Related Projects](#related-projects)
- [Author](#author)

---

## Overview

**BusinessDataChecker** automates the validation of business data by comparing two DataFrames — typically a source dataset and a target dataset — and running a series of integrity checks:

- **Row count verification** — ensures the number of records matches between datasets before and after a join.
- **Column value equality** — checks that values in corresponding columns are identical, reporting match counts and percentages.
- **Column type consistency** — verifies that column data types (e.g., StringType, IntegerType) are consistent between the left and right sides of the join.

The project is designed with **extensibility** in mind: new check types can be added by implementing a simple trait and registering it in the factory, without modifying the orchestration or entry-point code.

---

## Architecture

The project follows a **Strategy + Factory** architectural pattern:

- **Strategy** (`CheckHolder` trait) — defines a common interface for all check implementations.
- **Factory** (`CheckServiceFactory`) — maps check type codes to concrete strategy implementations.
- **Service** (`CheckService`, `DfService`) — orchestrates joins and check execution.
- **Domain** — case classes that encapsulate input configuration, intermediate results, and output check records.

For a detailed architecture diagram (PlantUML) and full component descriptions, see [Architecture.md](./Architecture.md).

```
External Libraries
     │
     ▼
┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│ SparkMain   │────▶│ DfService    │────▶│ Nageltrekker  │
│ (Entry)     │     │ (Join logic) │     │ (prepareDf)   │
└──────┬──────┘     └──────────────┘     └───────────────┘
       │
       ▼
┌──────────────┐     ┌──────────────────┐     ┌───────────────────┐
│ CheckService │────▶│ CheckService     │────▶│ CheckHolder       │
│ (Orchestr.)  │     │ Factory          │     │ (Strategy trait)  │
└──────────────┘     └──────────────────┘     └────┬────┬────┬────┘
                                                    │    │    │
                                           ┌────────┘    │    └────────┐
                                           ▼             ▼             ▼
                                    CheckCountRows  CheckEqualOnVal  CheckTypesEval
```

---

## Technology Stack

| Component              | Technology                     |
|------------------------|--------------------------------|
| **Build System**       | Maven 3.x                      |
| **JVM Languages**      | Scala 2.12.11, Java 1.8       |
| **Data Processing**    | Apache Spark SQL 2.4.5        |
| **Join Utilities**     | Nageltrekker 0.1.3 (GitHub)   |
| **Repository**         | Maven Central, jitpack.io      |

---

## Project Structure

```
businessdatachecker/
├── pom.xml                        # Parent POM (multi-module)
├── README.md                      # This file
├── Architecture.md                # Detailed architecture guide
└── joinatool/
    ├── pom.xml                    # Module POM
    └── src/
        ├── main/
        │   ├── java/com/boro/apps/joinatool/
        │   │   ├── CheckStatus.java    # Enum: NEW, ACTIVE, EXPIRED
        │   │   └── Codes.java          # Enum: COUNT_ROWS, EQUAL_ON_VAL, TYPES_EVAL
        │   └── scala/com/boro/apps/joinatool/
        │       ├── SparkMain.scala              # Entry point
        │       ├── checkimpl/
        │       │   ├── CheckHolder.scala        # Strategy trait
        │       │   ├── CheckCountRows.scala     # Row count check
        │       │   ├── CheckEqualOnVal.scala    # Value equality check
        │       │   └── CheckTypesEval.scala     # Type consistency check
        │       ├── checkservice/
        │       │   └── CheckService.scala       # Check orchestrator
        │       ├── dfservice/
        │       │   └── DfService.scala          # DataFrame join service
        │       ├── domain/
        │       │   └── DfAggregation.scala      # Domain case classes
        │       └── factory/
        │           └── CheckServiceFactory.scala # Factory mapping Codes→CheckHolder
        └── resources/              # (empty, available for future config)
```

---

## Getting Started

### Prerequisites

- **Java 8** (JDK 1.8)
- **Maven 3.x**
- **Apache Spark 2.4.5** (runtime)
- **Scala 2.12.11** (build-time, handled by scala-maven-plugin)

### Build

```bash
# Clone the repository
git clone https://github.com/Michael-Borovinskiy/BusinessDataChecker.git
cd BusinessDataChecker

# Build the entire project (parent POM + joinatool module)
mvn clean package
```

### Run

```bash
# Run the main class from the joinatool module
mvn exec:java -pl joinatool -Dexec.mainClass="com.boro.apps.joinatool.SparkMain"

# Or run the assembled JAR directly
java -cp joinatool/target/joinatool-1.0-SNAPSHOT.jar com.boro.apps.joinatool.SparkMain
```

> **Note:** The application creates a local SparkSession with `local[6]` master. On the first run, it automatically creates three Hive-style tables (`table1`, `table2`, `table3`) in a `spark-warehouse` directory with synthetic test data.

---

## How It Works

### Check Types

The system supports three check types, identified by the `Codes` enum:

| Check Code        | Description                                                                 | Pass Condition                                 |
|-------------------|-----------------------------------------------------------------------------|------------------------------------------------|
| `COUNT_ROWS`      | Compares row counts of left DF, right DF, and joined result DF             | All three counts are equal                     |
| `EQUAL_ON_VAL`    | For each column, compares values between left and right sides of the join  | Every column has 100% match                    |
| `TYPES_EVAL`      | For each column, compares data types between left and right sides          | Every column has matching types on both sides  |

### Data Flow

1. **Setup** — `SparkMain` initialises a `SparkSession` and creates test tables if they don't exist.
2. **Configuration** — A `DfAggregation` is created with a table name, left DataFrame, right DataFrame, and join key columns.
3. **Join** — `DfService` uses `Nageltrekker.prepareDf()` to merge the two DataFrames on the specified keys, producing a `TableResult` (or `TableResultWithPrepDF`).
4. **Check orchestration** — `CheckService` receives a `Codes` value, retrieves the corresponding `CheckHolder` implementation from `CheckServiceFactory`, executes the check, and wraps the result into a `Check` object (containing statistics, status, boolean result, and timestamp).
5. **Output** — Results are available in two forms:
   - **Textual details** via `getCheckDetails(code)` — returns `Map[String, Check]`.
   - **DataFrame statistics** via `getCheckDF(code)` — returns `TableStatistics` with a DataFrame containing per-column comparison details.

### Built-in Test Scenarios

In `SparkMain`, two comparison scenarios are executed:

1. **`table1` vs `table2`** — DFs with deliberate differences (null values, different percentages, different salaries). Most checks are expected to **fail**.
2. **`table1` vs `table3`** — Nearly identical DFs (table3 is a copy of table1 with one duplicate row). Checks are expected to **pass** (except `COUNT_ROWS` due to the duplicate).

---

## Module: `joinatool`

The `joinatool` module is a JAR that contains all the business logic. It depends on the Nageltrekker library for DataFrame join and comparison utilities.

### Entry Point

**`SparkMain.scala`** — Singleton object with the `main` method. It:
- Creates/initialises a `SparkSession` with `local[6]` master.
- On the first run, creates `table1`, `table2`, and `table3` with synthetic data via SQL `SELECT ... UNION ALL`.
- On subsequent runs, recreates the table references from the `spark-warehouse` directory.
- Instantiates `CheckService` with `DfService` for each comparison pair.
- Calls all three check types and prints results to stdout.

### Service Layer

- **`DfService`** — Accepts a `DfAggregation` and provides:
  - `joinResult`: Returns a merged DataFrame using `Nageltrekker.prepareDf()`.
  - `joinResultWithPrepDF`: Same as above but also exposes the original left/right DFs (required by `CheckCountRows`).

- **`CheckService`** — Accepts a `SparkSession` and `DfService`. Provides:
  - `getCheckDetails(code)`: Returns a `Map[String, Check]` with full check metadata.
  - `getCheckDF(code)`: Returns a `TableStatistics` with the comparison DataFrame.

### Check Implementations

All implementations extend the `CheckHolder` trait and override three methods:

| Implementation       | `getCalculationMap` Result                     | `getCalculationDf` Result           | `getCheckBool` Logic                         |
|----------------------|------------------------------------------------|-------------------------------------|----------------------------------------------|
| **`CheckCountRows`** | `Map[String, Long]` — counts for each DF       | Empty DataFrame                     | `dfLeft.count == dfRight.count == dfResult.count` |
| **`CheckEqualOnVal`**| `Map[String, (Long, Long)]` — (matches, match%) | DF with comparison details         | All columns have `matchPercent == 100`       |
| **`CheckTypesEval`** | `Map[String, (String, String)]` — (leftType, rightType) | DF with type comparison   | All columns have matching types on both sides |

### Factory

**`CheckServiceFactory`** — Maps `Codes` enum values to concrete `CheckHolder` implementations:

```scala
codes match {
  case Codes.TYPES_EVAL    => new CheckTypesEval
  case Codes.EQUAL_ON_VAL  => new CheckEqualOnVal
  case Codes.COUNT_ROWS    => new CheckCountRows
  case _ => throw new UnsupportedOperationException(...)
}
```

### Domain Model

| Case Class               | Purpose                                                    |
|--------------------------|------------------------------------------------------------|
| `DfAggregation`          | Input: table name, left DF, right DF, join key columns    |
| `TableResult`            | Result of `joinResult()`: table name + joined DF          |
| `TableResultWithPrepDF`  | Result of `joinResultWithPrepDF()`: also includes left/right DFs |
| `CheckStatistics`        | Check result as a generic map                              |
| `TableStatistics`        | Check result as a DataFrame                                |
| `Check`                  | Complete check record: code, statistics, status, result   |
| `Result`                 | Boolean pass/fail + timestamp                              |
| `Calculation`            | (Reserved for future use) Map of string to Any             |
| `ResultSet`              | (Reserved for future use) Calculation + Result             |

---

## Extending with New Checks

Adding a new business check requires **only 3 steps** — no orchestration code changes needed:

1. **Add a code** — Add a new value to `Codes.java` (e.g., `NULL_COUNT`).

2. **Implement the strategy** — Create a new class in `checkimpl/` extending `CheckHolder`:

   ```scala
   class CheckNullCount extends CheckHolder {
     override def getCalculationMap(spark: SparkSession, dfService: DfService): CheckStatistics = {
       // Compute statistics and return CheckStatistics
     }
     override def getCalculationDf(spark: SparkSession, dfService: DfService): TableStatistics = {
       // Return DataFrame with detailed statistics per column
     }
     override def getCheckBool(map: Map[_, _]): Boolean = {
       // Define pass/fail logic
     }
   }
   ```

3. **Register in factory** — Add a mapping in `CheckServiceFactory.getCheckImpl`:

   ```scala
   case Codes.NULL_COUNT => new CheckNullCount
   ```

The new check is now automatically available through `CheckService.getCheckDetails(Codes.NULL_COUNT)` and `CheckService.getCheckDF(Codes.NULL_COUNT)`.

---

## Dependencies

### External Libraries

| Artifact                    | Version   | Scope     | Description                                                          |
|-----------------------------|-----------|-----------|----------------------------------------------------------------------|
| `org.apache.spark:spark-sql_2.12` | 2.4.5 | compile   | Apache Spark SQL module for DataFrame operations                    |
| `org.scala-lang:scala-library`    | 2.12.11   | compile   | Scala standard library                                               |
| `com.github.Michael-Borovinskiy:Nageltrekker` | 0.1.3 | compile | DataFrame join and comparison utilities (via jitpack.io)            |

### Nageltrekker Library

The **Nageltrekker** library (by the same author) provides core DataFrame comparison functions used by `DfService` and check implementations:

- **`prepareDf(dfLeft, dfRight, joinCols)`** — Joins two DataFrames on specified columns, renaming columns with `_L`/`_R` suffixes to avoid ambiguity.
- **`checkEqualColumns(joinedDf)`** — For each column pair, counts matching rows and calculates the match percentage, returning both a summary map and a detail DataFrame.
- **`checkEqualColumnTypes(spark, joinedDf)`** — Extracts and compares the Spark SQL data types of each column pair, returning a comparison map and DataFrame.

---

## Related Projects

- **[Nageltrekker](https://github.com/Michael-Borovinskiy/Nageltrekker)** — Spark DataFrame comparison library used by this project.
- **[BusinessDataChecker](https://github.com/Michael-Borovinskiy/BusinessDataChecker)** — This project on GitHub.

---

## Author

**Michael Borovinskiy**

- GitHub: [@Michael-Borovinskiy](https://github.com/Michael-Borovinskiy)