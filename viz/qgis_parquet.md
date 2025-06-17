## I found a (Delta) Table in the (Unity) Catalog that contains geospatial data (WKB/WKT column), can I download it to my computer to open in QGIS?

The short answer is yes. You can export the table into Volumes into a parquet directory. If you use `coalesce(1)`, then besides the metadata files there will only be one file named `part-00000-*.parquet` (where `*` stands for arbitrary other characters). If you download this file, with a bit of luck, you can open it in QGIS.

```python
%python
TABLENAME = None  #FILL_IN: your table name
CATALOG = None  #FILL_IN
SCHEMA = None  #FILL_IN
VOLUME = None  #FILL_IN
VOLUME_PATH = None  #FILL_IN: path within the volume

PARQUET_OUT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{VOLUME_PATH.strip('/')}/{TABLENAME}.parquet"

spark.table(TABLENAME).coalesce(1).write.parquet(PARQUET_OUT)
```
Now if you navigate in the Catalog Explorer to the Volume and the chosen volume path, you can download the file to your desktop.

Further details: QGIS uses GDAL to open your parquet file, and makes some [assumptions](https://gdal.org/en/stable/drivers/vector/parquet.html), such as that your geometry is stored in WKB in a column called 'geometry', or it is WKT in a column including `wkt` in its name. You can further adjust this if needed in QGIS in the reading options parameter `GEOM_POSSIBLE_NAMES`. Similarly you can define or override the `CRS` if needed.

If the file you created takes too long to visualize, two things you can do are:
- use `ogr2ogr` or DuckDB Spatial to write out a Flatgeobuf. It will be an even larger file, but will be rendered faster in QGIS.
- Partition your parquet file by e.g. the H3 spatial index, and use one file per cell.
