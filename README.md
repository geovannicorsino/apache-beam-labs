# Apache Beam Labs

A hands-on learning project exploring Apache Beam concepts through progressively complex examples, following the official documentation and Tour of Beam guide.

## Requirements

- Python 3.x
- `apache-beam==2.71.0`

```bash
pip install -r requirements.txt
```

## Project Structure

```
src/
├── common_transforms/       # Built-in aggregation transforms
├── core_transforms/         # Core pipeline patterns
│   ├── map/                 # Mapping and grouping operations
│   └── combine/             # Combine and aggregation patterns
├── io_connectors/           # Reading and writing data
├── challenges/              # Exercises based on Tour of Beam
└── case_studies/            # Real-world pipeline examples
```

## Examples

### Common Transforms

| File | Concept |
|------|---------|
| `common_transforms/count.py` | `Count.PerElement()` — count occurrences per element |
| `common_transforms/filter.py` | `Filter` with side inputs (`AsIter`) |
| `common_transforms/mean.py` | `Mean.Globally()` |
| `common_transforms/sum.py` | `CombinePerKey` with `sum` |
| `common_transforms/max.py` | `CombinePerKey` with `max` |
| `common_transforms/min.py` | `CombinePerKey` with `min` |
| `common_transforms/with_keys.py` | `WithKeys` to build key-value pairs |

### Core Transforms — Map

| File | Concept |
|------|---------|
| `core_transforms/map/map.py` | Basic `Map` transformation |
| `core_transforms/map/pardo_one_to_one.py` | `ParDo` (DoFn) one-to-one |
| `core_transforms/map/pardo_one_to_many.py` | `ParDo` one-to-many |
| `core_transforms/map/flat_map.py` | `FlatMap` to explode records |
| `core_transforms/map/group_by_key.py` | `GroupByKey` |
| `core_transforms/map/co_group_key.py` | `CoGroupByKey` (join two PCollections) |

### Core Transforms — Combine

| File | Concept |
|------|---------|
| `core_transforms/combine/combine.py` | `CombineGlobally` with built-in function |
| `core_transforms/combine/combine_fn.py` | Custom `CombineFn` class |
| `core_transforms/combine/combine_by_key.py` | `CombinePerKey` with custom `CombineFn` |

### Pipeline Patterns

| File | Concept |
|------|---------|
| `core_transforms/branching.py` | Branching pipelines + `Flatten` |
| `core_transforms/composite_transform.py` | Reusable `PTransform` subclass |
| `core_transforms/side_input.py` | Side inputs with `AsSingleton` |
| `core_transforms/additional_outputs.py` | Multiple outputs with `TaggedOutput` |

### I/O

| File | Concept |
|------|---------|
| `io_connectors/write_text.py` | `WriteToText` |

### Challenges

| File | Description |
|------|-------------|
| `challenges/word_count.py` | Word count on King Lear text |
| `challenges/tour_of_beam/1_challenge.py` | Taxi orders: group and sum by fare threshold |

### Case Studies

| File | Description |
|------|-------------|
| `case_studies/identify_banks_defaulter_customers/skippers_defaulters.py` | Identify credit card defaulters using a scoring pipeline |

## Running an Example

```bash
python src/core_transforms/combine/combine_by_key.py
```

## Key Concepts Covered

- **Transforms**: `Map`, `FlatMap`, `Filter`, `ParDo`
- **Aggregations**: `CombineGlobally`, `CombinePerKey`, `Count`, `Mean`, `Sum`, `Max`, `Min`
- **Grouping**: `GroupByKey`, `CoGroupByKey`, `WithKeys`
- **Advanced**: Side inputs, multiple outputs, composite transforms, custom `CombineFn`
- **I/O**: `ReadFromText`, `WriteToText`, CSV parsing
