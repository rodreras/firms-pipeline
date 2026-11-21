# NASA FIRMS active-fire detections in South America

This collection contains satellite detections of active fires and other thermal anomalies. Use it to study where and when satellites detect heat. Each annual Item links to detection points and H3 summaries at resolutions 4 and 5.

## Coverage and source

The default request box spans 82.44° W to 34.10° W and 56.46° S to 13.58° N. The box defines the request area. Check `firms:request_bbox` on the annual Item for the recorded request box.

NASA FIRMS supplies MODIS detections and VIIRS detections from Suomi NPP, NOAA-20, and NOAA-21. The pipeline preserves the source product and version. Product availability varies by date. A year directory can contain part of a year. Check `firms:coverage` for product intervals when the Item provides it.

Two products can report the same fire. One fire can produce multiple detections over time. Detection counts measure satellite detections. Use a burned-area product to estimate burned area.

## Detection points

`occurrences.parquet` stores GeoParquet 1.1 with WGS 84 point geometry. The file can include rows with only `acq_date` populated. Use `geom IS NOT NULL` to select detections. Check product coverage before you interpret a date without detections.

| Columns | Meaning |
| --- | --- |
| `longitude`, `latitude` | Detection coordinates in decimal degrees. |
| `geom`, `bbox` | WKB point and its bounds in WGS 84. |
| `acq_date`, `acq_time`, `acquired_at` | UTC date, HHMM text, and UTC timestamp. |
| `source`, `satellite`, `instrument`, `version` | FIRMS product identifier and source provenance. |
| `confidence` | Source text. VIIRS uses `l`, `n`, and `h`. MODIS uses scores from 0 to 100. |
| `scan`, `track` | Pixel dimensions in kilometers. |
| `bright_ti4`, `bright_ti5` | VIIRS band temperatures in kelvin. |
| `brightness`, `bright_t31` | MODIS channel temperatures in kelvin. |
| `frp` | Fire radiative power in megawatts. |
| `daynight` | FIRMS day or night flag. |
| `source_attributes` | Additional source fields as JSON text. |

Temperature columns for the other instrument are null. Fields such as FRP can also be null. Consult the STAC asset column metadata for types.

## H3 summaries

`aggregate-l4.parquet` and `aggregate-l5.parquet` group detections by UTC year and H3 cell. Each file covers one resolution. Both files summarize the same detections.

| Columns | Meaning |
| --- | --- |
| `year`, `h3_cell` | UTC observation year and numeric H3 cell index. |
| `geom` | WKB cell polygon in WGS 84. |
| `detection_count` | Number of detections in the group. |
| `frp_count` | Number of detections with measured FRP. |
| `frp_sum`, `frp_avg`, `frp_max` | Sum, mean, and maximum of measured FRP in megawatts. |

If no detection in a group has measured FRP, the FRP count is zero. Its sum, mean, and maximum are null.

## Data use

The pipeline derives these files from NASA FIRMS source observations. See NASA's data-use policy for terms and citation guidance.

- [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/)
- [NASA Earthdata data-use policy](https://www.earthdata.nasa.gov/data/use-policy)

## Annual maps

When an annual Item has map assets, select an H3 resolution and a MapLibre style. Styles show detection count, total FRP, mean FRP, or maximum FRP. Gray cells have no value for the selected measure. Each style uses breaks from its own year and resolution. Read the break values before you compare colors between years or resolutions. Use the GeoParquet files for analytical totals.
