# Based on https://gist.github.com/Maxxen/37e4a9f8595ea5e6a20c0c8fbbefe955
# by Max Gabrielsson

import os

import duckdb
import flask
from databricks import sql  # type: ignore

MAX_FEATURES_PER_TILE = 30_000

# Initialize Flask app
app = flask.Flask(__name__)

config = {"allow_unsigned_extensions": "true"}
duckdb_con = duckdb.connect(config=config)

duckdb_con.execute("INSTALL spatial")

duckdb_con.execute("load spatial")


dbx_con = sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN"),
)


# Tile endpoint to serve vector tiles
@app.route("/tiles/<int:z>/<int:x>/<int:y>.pbf")
def get_tile(z, x, y):
    # Query to get the tile data from DuckDB
    # - Note that the geometry is assumed to be projected to `EPSG:3857` (Web Mercator)

    # Use con.cursor() to avoid threading issues with Flask
    with duckdb_con.cursor() as local_con:
        tileenv = local_con.execute(
            """
            select st_astext(st_transform(
            st_tileenvelope($1, $2, $3),
            'EPSG:3857',
            'OGC:CRS84'
            ))
            """,
            [z, x, y],
        ).fetchone()

    query = f"""
        select
         st_aswkb(geometry) as geometry
        from
        `workspace`.`default`.`building_geom`
            where st_intersects(geometry, st_geomfromtext('{tileenv[0]}'))
            limit {MAX_FEATURES_PER_TILE}"""  # noqa: S608

    with dbx_con.cursor() as cursor:
        cursor.execute(query)
        da = cursor.fetchall_arrow()  # noqa: F841

    # Use con.cursor() to avoid threading issues with Flask
    with duckdb_con.cursor() as local_con:
        tile_blob = None
        tile_count = local_con.execute(
            """
            select count(*) cnt from da
            """
        ).fetchone()[0]
        if tile_count == MAX_FEATURES_PER_TILE:
            # If we hit the limit, return an empty tile to avoid incomplete data
            tile_blob = local_con.execute(
                """
                select ST_AsMVT({
                    "geometry": ST_AsMVTGeom(
                        ST_TileEnvelope($1, $2, $3),
                        ST_Extent(ST_TileEnvelope($1, $2, $3))
                        )
                    }) 
                """,
                [z, x, y],
            ).fetchone()
        else:
            tile_blob = local_con.execute(
                """
                select ST_AsMVT({
                    "geometry": ST_AsMVTGeom(
                        st_transform(
                            st_geomfromwkb(geometry),
                            'OGC:CRS84',
                            'EPSG:3857'),
                        ST_Extent(ST_TileEnvelope($1, $2, $3))
                        )
                    }) from da
                """,
                [z, x, y],
            ).fetchone()

        # Send the tile data as a response
        tile = tile_blob[0] if tile_blob and tile_blob[0] else b""
        return flask.Response(tile, mimetype="application/x-protobuf")


# HTML content for the index page
INDEX_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Vector Tile Viewer</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <script src='https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js'></script>
    <link href='https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css'
        rel='stylesheet' />
    <style>
        body { margin: 0; padding: 0; }
        #map { position: absolute; top: 0; bottom: 0; width: 100%; }
    </style>
</head>
<body>
<div id="map"></div>
<script>
    const map = new maplibregl.Map({
        container: 'map',
        style: {
            version: 8,
            sources: {
                'buildings': {
                    type: 'vector',
                    tiles: [`${window.location.origin}/tiles/{z}/{x}/{y}.pbf`]
                },
                // Also use a public open source basemap
                'osm': {
                    type: 'raster',
                    tiles: [
                        'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
                        'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
                        'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'
                    ],
                    tileSize: 256
                }
            },
            layers: [
                {
                    id: 'background',
                    type: 'background',
                    paint: { 'background-color': '#a0c8f0' }
                },
                {
                    id: 'osm',
                    type: 'raster',
                    source: 'osm'
                },
                {
                    id: 'buildings-fill',
                    type: 'fill',
                    source: 'buildings',
                    'source-layer': 'layer',
                    paint: {
                        'fill-color': 'blue',
                        'fill-opacity': 0.6,
                        'fill-outline-color': '#ffffff'
                    }
                },
                {
                    id: 'buildings-stroke',
                    type: 'line',
                    source: 'buildings',
                    'source-layer': 'layer',
                    paint: {
                        'line-color': 'black',
                        'line-width': 0.5
                    }
                }
            ]
        },
        // Zoom in on amf
        center: [5.38327, 52.15660],
        zoom: 12,
        prefetchZoomDelta: 0, // disables zoom-level prefetch
        refreshExpiredTiles: false, // don’t re-request tiles that have expired

    });

    map.addControl(new maplibregl.NavigationControl());

    // Add click handler to show feature properties
    map.on('click', 'buildings-fill', (e) => {
        const coordinates = e.lngLat;
        const properties = e.features[0].properties;

        let popupContent = '<h3>Building Properties</h3>';
        for (const [key, value] of Object.entries(properties)) {
            popupContent += `<p><strong>${key}:</strong> ${value}</p>`;
        }

        new maplibregl.Popup()
            .setLngLat(coordinates)
            .setHTML(popupContent)
            .addTo(map);
    });

    // Change cursor on hover
    map.on('mouseenter', 'buildings-fill', () => {
        map.getCanvas().style.cursor = 'pointer';
    });

    map.on('mouseleave', 'buildings-fill', () => {
        map.getCanvas().style.cursor = '';
    });


// ---- Throttle building tile loading ----
let reloadTimeout;

function removeBuildingLayers() {
    if (map.getLayer('buildings-fill')) map.removeLayer('buildings-fill');
    if (map.getLayer('buildings-stroke')) map.removeLayer('buildings-stroke');
    if (map.getSource('buildings')) map.removeSource('buildings');
}

function addBuildingLayers() {
    if (map.getSource('buildings')) return;

    map.addSource('buildings', {
        type: 'vector',
        tiles: [`${window.location.origin}/tiles/{z}/{x}/{y}.pbf`]
    });

    map.addLayer({
        id: 'buildings-fill',
        type: 'fill',
        source: 'buildings',
        'source-layer': 'layer',
        paint: {
            'fill-color': 'blue',
            'fill-opacity': 0.6,
            'fill-outline-color': '#ffffff'
        }
    });

    map.addLayer({
        id: 'buildings-stroke',
        type: 'line',
        source: 'buildings',
        'source-layer': 'layer',
        paint: {
            'line-color': 'black',
            'line-width': 0.5
        }
    });
}

// When user starts moving or zooming
function onInteractionStart() {
    clearTimeout(reloadTimeout);
    removeBuildingLayers();
}

// When user stops moving or zooming
function onInteractionEnd() {
    clearTimeout(reloadTimeout);
    reloadTimeout = setTimeout(() => {
        addBuildingLayers();
    }, 2000);
}

// Bind to move & zoom events
map.on('movestart', onInteractionStart);
map.on('moveend', onInteractionEnd);
map.on('zoomstart', onInteractionStart);
map.on('zoomend', onInteractionEnd);

</script>
</body>
</html>
"""


# Serve the static HTML file for the index page
@app.route("/")
def index():
    return flask.Response(INDEX_HTML, mimetype="text/html")


if __name__ == "__main__":
    # Start on localhost
    app.run(debug=True)  # noqa: S201
