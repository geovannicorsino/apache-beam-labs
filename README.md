# Apache Beam Labs 🚀

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.71.0-orange.svg)](https://beam.apache.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![CI](https://github.com/geovannicorsino/apache-beam-labs/actions/workflows/ci.yml/badge.svg)](https://github.com/geovannicorsino/apache-beam-labs/actions/workflows/ci.yml)

A hands-on learning project exploring Apache Beam concepts through progressively complex examples, following the official documentation and Tour of Beam guide. Perfect for developers looking to master data processing pipelines with Apache Beam!

## ✨ Features

- 📚 Comprehensive examples covering core transforms, windowing, and I/O
- 🏗️ Real-world case studies and challenges
- 🔄 Progressive complexity from basics to advanced patterns
- 📖 Follows official Apache Beam Tour of Beam curriculum

## 📋 Requirements

- Python 3.12
- [uv](https://docs.astral.sh/uv/) (package manager)
- Apache Beam 2.71.0

## 🗂️ Project Structure

```
src/
├── common_transforms/       # Built-in aggregation transforms 🧮
├── core_transforms/         # Core pipeline patterns 🔧
│   ├── map/                 # Mapping and grouping operations 🗺️
│   └── combine/             # Combine and aggregation patterns ➕
├── advanced_transforms/     # Advanced pipeline patterns 🧠
├── windowing/               # Streaming windowing strategies ⏱️
├── triggers/                # Streaming trigger strategies
├── io_connectors/           # Reading and writing data 📥📤
├── challenges/              # Exercises based on Tour of Beam 🎯
└── case_studies/            # Real-world pipeline examples 💼
```

## 🚀 Quick Start

1. Clone the repository
2. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
3. Install dependencies (cria o `.venv` e resolve o `uv.lock` automaticamente):
   ```bash
   uv sync --group dev
   ```
4. Run an example:
   ```bash
   uv run python src/common_transforms/count.py
   ```

## 📖 Examples

### Common Transforms 🧮

| File                                                                   | Concept                    | Description                       |
| ---------------------------------------------------------------------- | -------------------------- | --------------------------------- |
| [`common_transforms/count.py`](src/common_transforms/count.py)         | `Count.PerElement()`       | Count occurrences per element     |
| [`common_transforms/filter.py`](src/common_transforms/filter.py)       | `Filter` with side inputs  | Filter using `AsIter` side inputs |
| [`common_transforms/mean.py`](src/common_transforms/mean.py)           | `Mean.Globally()`          | Calculate global mean             |
| [`common_transforms/sum.py`](src/common_transforms/sum.py)             | `CombinePerKey` with `sum` | Sum values per key                |
| [`common_transforms/max.py`](src/common_transforms/max.py)             | `CombinePerKey` with `max` | Find max per key                  |
| [`common_transforms/min.py`](src/common_transforms/min.py)             | `CombinePerKey` with `min` | Find min per key                  |
| [`common_transforms/with_keys.py`](src/common_transforms/with_keys.py) | `WithKeys`                 | Build key-value pairs             |

### Core Transforms — Map 🗺️

| File                                                                                       | Concept             | Description                            |
| ------------------------------------------------------------------------------------------ | ------------------- | -------------------------------------- |
| [`core_transforms/map/map.py`](src/core_transforms/map/map.py)                             | Basic `Map`         | Simple element transformation          |
| [`core_transforms/map/pardo_one_to_one.py`](src/core_transforms/map/pardo_one_to_one.py)   | `ParDo` one-to-one  | One input to one output                |
| [`core_transforms/map/pardo_one_to_many.py`](src/core_transforms/map/pardo_one_to_many.py) | `ParDo` one-to-many | One input to multiple outputs          |
| [`core_transforms/map/flat_map.py`](src/core_transforms/map/flat_map.py)                   | `FlatMap`           | Explode records into multiple elements |
| [`core_transforms/map/group_by_key.py`](src/core_transforms/map/group_by_key.py)           | `GroupByKey`        | Group by key                           |
| [`core_transforms/map/co_group_key.py`](src/core_transforms/map/co_group_key.py)           | `CoGroupByKey`      | Join two PCollections                  |

### Core Transforms — Combine ➕

| File                                                                                         | Concept            | Description                                 |
| -------------------------------------------------------------------------------------------- | ------------------ | ------------------------------------------- |
| [`core_transforms/combine/combine.py`](src/core_transforms/combine/combine.py)               | `CombineGlobally`  | Global combination with built-in function   |
| [`core_transforms/combine/combine_fn.py`](src/core_transforms/combine/combine_fn.py)         | Custom `CombineFn` | Custom combination logic                    |
| [`core_transforms/combine/combine_by_key.py`](src/core_transforms/combine/combine_by_key.py) | `CombinePerKey`    | Per-key combination with custom `CombineFn` |

### Pipeline Patterns 🔄

| File                                                                                   | Concept               | Description                                                 |
| -------------------------------------------------------------------------------------- | --------------------- | ----------------------------------------------------------- |
| [`core_transforms/branching.py`](src/core_transforms/branching.py)                     | Branching + `Flatten` | Split and merge pipelines                                   |
| [`core_transforms/flatten.py`](src/core_transforms/flatten.py)                         | `Flatten`             | Merge multiple PCollections into one                        |
| [`core_transforms/composite_transform.py`](src/core_transforms/composite_transform.py) | Reusable `PTransform` | Custom composite transforms                                 |
| [`core_transforms/side_input.py`](src/core_transforms/side_input.py)                   | Side inputs           | Using `AsSingleton` side inputs                             |
| [`core_transforms/additional_outputs.py`](src/core_transforms/additional_outputs.py)   | Multiple outputs      | `TaggedOutput` for multiple outputs                         |
| [`core_transforms/partition.py`](src/core_transforms/partition.py)                     | `Partition`           | Split a PCollection into N disjoint sub-collections         |

### Advanced Transforms 🧠

| File                                                                                           | Concept            | Description                                                     |
| ---------------------------------------------------------------------------------------------- | ------------------ | --------------------------------------------------------------- |
| [`advanced_transforms/stateful_dofn.py`](src/advanced_transforms/stateful_dofn.py)             | Stateful `DoFn`    | Per-key persistent state for deduplication                      |
| [`advanced_transforms/splittable_dofn.py`](src/advanced_transforms/splittable_dofn.py)         | Splittable `DoFn`  | Process large elements in parallel by splitting into sub-ranges |
| [`advanced_transforms/dead_letter_queue.py`](src/advanced_transforms/dead_letter_queue.py)     | Dead Letter Queue  | Route invalid elements to a separate error output               |
| [`advanced_transforms/reshuffle.py`](src/advanced_transforms/reshuffle.py)                     | `Reshuffle`        | Break stage fusion to redistribute elements across workers      |

### Windowing (Streaming) ⏱️

| File                                                             | Window Type      | Use Case                        |
| ---------------------------------------------------------------- | ---------------- | ------------------------------- |
| [`windowing/global_window.py`](src/windowing/global_window.py)   | `GlobalWindows`  | Process each event individually |
| [`windowing/fixed_window.py`](src/windowing/fixed_window.py)     | `FixedWindows`   | System metrics per minute       |
| [`windowing/sliding_window.py`](src/windowing/sliding_window.py) | `SlidingWindows` | Rolling averages                |
| [`windowing/session_window.py`](src/windowing/session_window.py) | `Sessions`       | User session analytics          |

### Triggers (Streaming)

| File                                                                         | Trigger                  | Description                                    |
| ---------------------------------------------------------------------------- | ------------------------ | ---------------------------------------------- |
| [`triggers/after_watermark.py`](src/triggers/after_watermark.py)             | `AfterWatermark`         | Event time trigger with early and late firings |
| [`triggers/repeatedly.py`](src/triggers/repeatedly.py)                       | `Repeatedly(AfterCount)` | Data-driven trigger: fires every N elements    |
| [`triggers/after_processing_time.py`](src/triggers/after_processing_time.py) | `AfterProcessingTime`    | Processing time trigger: fires every N seconds |
| [`triggers/composite_trigger.py`](src/triggers/composite_trigger.py)         | `AfterFirst`             | Combine triggers with OR                       |

### I/O 📥📤

| File                                                                                         | Concept                    | Description                               |
| -------------------------------------------------------------------------------------------- | -------------------------- | ----------------------------------------- |
| [`io_connectors/text-io/read_text.py`](src/io_connectors/text-io/read_text.py)               | `ReadFromText`             | Read lines from a local text file         |
| [`io_connectors/text-io/read_gcs.py`](src/io_connectors/text-io/read_gcs.py)                 | `ReadFromText (GCS)`       | Read lines from a GCS text file           |
| [`io_connectors/text-io/write_text.py`](src/io_connectors/text-io/write_text.py)             | `WriteToText`              | Write a PCollection to a local text file  |
| [`io_connectors/text-io/write_gcs.py`](src/io_connectors/text-io/write_gcs.py)               | `WriteToText (GCS)`        | Write a PCollection to a GCS text file    |
| [`io_connectors/bigqueryio/read_table_bq.py`](src/io_connectors/bigqueryio/read_table_bq.py) | `ReadFromBigQuery (Table)` | Read all rows from a BigQuery table       |
| [`io_connectors/bigqueryio/read_query_bq.py`](src/io_connectors/bigqueryio/read_query_bq.py) | `ReadFromBigQuery (Query)` | Read results of a SQL query from BigQuery |
| [`io_connectors/bigqueryio/write_bq.py`](src/io_connectors/bigqueryio/write_bq.py)           | `WriteToBigQuery`          | Write a PCollection to a BigQuery table   |
| [`io_connectors/avro/write_avro.py`](src/io_connectors/avro/write_avro.py)                   | `WriteToAvro`              | Write a PCollection to an Avro file       |
| [`io_connectors/avro/read_avro.py`](src/io_connectors/avro/read_avro.py)                     | `ReadFromAvro`             | Read records from an Avro file            |
| [`io_connectors/parquet/write_parquet.py`](src/io_connectors/parquet/write_parquet.py)       | `WriteToParquet`           | Write a PCollection to a Parquet file     |
| [`io_connectors/parquet/read_parquet.py`](src/io_connectors/parquet/read_parquet.py)         | `ReadFromParquet`          | Read records from a Parquet file          |

### Challenges 🎯

| File                                                                                   | Description                                  |
| -------------------------------------------------------------------------------------- | -------------------------------------------- |
| [`challenges/word_count.py`](src/challenges/word_count.py)                             | Word count on King Lear text                 |
| [`challenges/tour_of_beam/1_challenge.py`](src/challenges/tour_of_beam/1_challenge.py) | Taxi orders: group and sum by fare threshold |

### Case Studies 💼

| File                                                                                                                                                   | Description                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------ |
| [`case_studies/identify_banks_defaulter_customers/skippers_defaulters.py`](src/case_studies/identify_banks_defaulter_customers/skippers_defaulters.py) | Identify credit card defaulters using scoring pipeline |

## 🏃‍♂️ Running an Example

To run any example, navigate to the project root and execute:

```bash
uv run python src/<path_to_example>.py
```

For example:

```bash
uv run python src/common_transforms/count.py
```

## 🧪 Tests

Unit tests use Apache Beam's `TestPipeline` com `assert_that` para validar outputs sem side effects.

```bash
uv run pytest
```

Lint e formatação:

```bash
uv run ruff check .
uv run ruff format --check .
```

| File                                                                       | What it tests                                      |
| -------------------------------------------------------------------------- | -------------------------------------------------- |
| [`tests/test_combine_fns.py`](tests/test_combine_fns.py)                   | `SumEvenFn` and `SumEvenOrOddFn` accumulator logic |
| [`tests/test_routing.py`](tests/test_routing.py)                           | `RouteRecords` (TaggedOutput) and `ParseAndValidateDoFn` (Dead Letter Queue) |
| [`tests/test_side_input.py`](tests/test_side_input.py)                     | `EnrichDoFn` with `AsSingleton` side inputs        |
| [`tests/test_stateful_dofn.py`](tests/test_stateful_dofn.py)               | `DeduplicateByKeyDoFn` stateful deduplication      |
| [`tests/test_metrics_combine_fn.py`](tests/test_metrics_combine_fn.py)     | `MetricsCombineFn` windowed metric aggregation     |

## Key Concepts Covered

- **Transforms**: `Map`, `FlatMap`, `Filter`, `ParDo`, `Partition`, `Reshuffle`
- **Aggregations**: `CombineGlobally`, `CombinePerKey`, `Count`, `Mean`, `Sum`, `Max`, `Min`
- **Grouping**: `GroupByKey`, `CoGroupByKey`, `WithKeys`
- **Advanced**: Side inputs, multiple outputs, composite transforms, custom `CombineFn`, Splittable DoFn, Stateful DoFn, Dead Letter Queue
- **Windowing**: `GlobalWindows`, `FixedWindows`, `SlidingWindows`, `Sessions`
- **Triggers**: `AfterWatermark`, `AfterCount`, `AfterProcessingTime`, `AfterFirst`, `Repeatedly`, accumulation modes
- **I/O**: `ReadFromText`, `WriteToText`, `ReadFromBigQuery`, `WriteToBigQuery`, `ReadFromAvro`, `WriteToAvro`, `ReadFromParquet`, `WriteToParquet`
