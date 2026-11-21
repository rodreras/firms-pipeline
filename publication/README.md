# South America geospatial catalog

This catalog publishes NASA FIRMS active-fire and thermal-anomaly detections for the South America request area. The occurrences collection contains annual detection points and H3 summaries. The summaries help compare detection counts and fire radiative power across regions and years.

Open [catalog.json](catalog.json) to find the collection. The [occurrences guide](occurrences/README.md) explains the source products, coverage, and fields. Each annual STAC Item lists the files for that year.

## Source and data use

The pipeline downloads observations from the NASA Fire Information for Resource Management System. It converts the source records to GeoParquet and derives H3 summaries from the detection points. See NASA's data-use policy for terms and citation guidance.

- [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/)
- [NASA Earthdata data-use policy](https://www.earthdata.nasa.gov/data/use-policy)
- [FIRMS pipeline repository](https://github.com/rodreras/firms-pipeline)
