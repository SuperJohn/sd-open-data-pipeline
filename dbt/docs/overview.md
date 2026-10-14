{% docs __overview__ %}

# San Diego Get It Done 311 pipeline

dbt lineage and documentation for the City of San Diego **Get It Done** (311)
service-request analytics warehouse.

## Layers

| Layer | Schema | Role |
|---|---|---|
| raw | `raw` | Landing zone — all VARCHAR plus `_source_file` / `_loaded_at`; council district polygons as GeoJSON text |
| staging | `staging` | Typed, trimmed, deduped requests; district polygons as DuckDB spatial GEOMETRY |
| intermediate | `intermediate` | Lifecycle, SLA, geo-validity, and point-in-polygon council district enrichment |
| marts | `marts` | Analytics-ready facts and district/service scorecards |

## How to regenerate

```bash
make ingest SOURCE=sample
make dbt-build
make docs          # writes site/ for local preview or Pages deploy
```

Source data: [data.sandiego.gov — Get It Done](https://data.sandiego.gov/datasets/get-it-done-311/)
and [City Council Districts (GIS)](https://data.sandiego.gov/datasets/gis-city-council-districts/).
Code: [SuperJohn/sd-open-data-pipeline](https://github.com/SuperJohn/sd-open-data-pipeline).

{% enddocs %}
