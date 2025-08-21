// PMTiles Viewer Application
// Initialize the PMTiles protocol and create the map

// Add the PMTiles plugin to the maplibregl global
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

// Configuration object - will be populated from API
let config = {
    pmtilesUrl: '',
    baseMapUrl: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    center: [0, 0],
    zoom: 2
};

// Create a popup for displaying feature properties
const popup = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: false,
    maxWidth: '400px'
});

// Fetch configuration from the server
async function initializeMap() {
    try {
        // Get parameters from URL or use defaults
        const urlParams = new URLSearchParams(window.location.search);
        const filePath = urlParams.get('filePath');
        const sourceLayer = urlParams.get('sourceLayer');

        console.log('Raw URL parameters:', { filePath, sourceLayer });

        // Build API URL with parameters
        const apiUrl = `/api/config?filePath=${encodeURIComponent(filePath)}&sourceLayer=${encodeURIComponent(sourceLayer)}`;
        console.log('API URL:', apiUrl);

        const response = await fetch(apiUrl);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        config = await response.json();

        // Create a new PMTiles instance
        const p = new pmtiles.PMTiles(config.pmtilesUrl);

        // Share one instance across the JS code and the map renderer
        protocol.add(p);

        // Fetch the header to get the center coordinates and zoom level
        p.getHeader().then(h => {
            const map = new maplibregl.Map({
                container: 'map',
                zoom: config.zoom || h.maxZoom - 2,
                center: config.center || [h.centerLon, h.centerLat],
                style: {
                    version: 8,
                    sources: {
                        // Base map source
                        'osm': {
                            type: 'raster',
                            tiles: [config.baseMapUrl],
                            tileSize: 256,
                            attribution: '© <a href="https://openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                        },
                        // PMTiles source
                        'example_source': {
                            type: 'vector',
                            url: `pmtiles://${config.pmtilesUrl}`,
                        }
                    },
                    layers: [
                        // Base map layer (rendered first, behind everything)
                        {
                            'id': 'osm-tiles',
                            'type': 'raster',
                            'source': 'osm',
                            'minzoom': 0,
                            'maxzoom': 22
                        },
                        // PMTiles layers (rendered on top of base map)
                        {
                            'id': 'geometries',
                            'type': 'fill',
                            'source': 'example_source',
                            'source-layer': config.sourceLayer,  // use from config
                            paint: {
                                "fill-color": "steelblue",
                                "fill-opacity": 0.7,
                                'fill-outline-color': '#000000'  // black border for better visibility
                            },
                            filter: ["==", ["geometry-type"], "Polygon"],

                        },
                        {
                            'id': 'line',
                            'type': 'line',
                            'source': 'example_source',
                            'source-layer': config.sourceLayer,  // use from config
                            paint: {
                                "line-color": "steelblue",
                                "line-width": 2,
                            },
                            filter: ["==", ["geometry-type"], "LineString"],
                        },
                        {
                            'id': 'circle',
                            'type': 'circle',
                            'source': 'example_source',
                            'source-layer': config.sourceLayer,  // use from config
                            paint: {
                                "circle-color": "steelblue",
                                "circle-radius": ["interpolate", ["linear"], ["zoom"], 4, 2, 12, 4],
                                "circle-opacity": 0.5,
                                "circle-stroke-color": "white",
                                "circle-stroke-width": [
                                    "case",
                                    ["boolean", ["feature-state", "hover"], false],
                                    3,
                                    0,
                                ],
                            },
                            filter: ["==", ["geometry-type"], "Point"],
                        },
                    ]
                }
            });

            // Optional: Add event listeners for map interactions
            map.on('load', () => {
                console.log('Map loaded successfully');
            });

            map.on('error', (e) => {
                console.error('Map error:', e);
            });

            // Add click event handler for PMTiles objects
            map.on('click', 'geometries', (e) => {
                // Prevent the click from propagating to the map
                e.preventDefault();

                // Get the clicked feature
                const feature = e.features[0];
                if (!feature) return;

                // Get the coordinates of the click
                const coordinates = e.lngLat;

                // Create HTML content for the popup
                let popupContent = '<div style="font-family: Arial, sans-serif; max-width: 350px;">';
                popupContent += '<h3 style="margin: 0 0 10px 0; color: #333;">Feature Properties</h3>';

                // Display all properties of the feature
                if (feature.properties) {
                    const properties = feature.properties;
                    popupContent += '<div class="popup-scroll" style="max-height: 300px; overflow-y: auto;">';

                    // Sort properties alphabetically for better organization
                    const sortedKeys = Object.keys(properties).sort();

                    sortedKeys.forEach(key => {
                        const value = properties[key];
                        // Format the value for display
                        let displayValue = value;
                        if (typeof value === 'number') {
                            displayValue = value.toLocaleString();
                        } else if (typeof value === 'boolean') {
                            displayValue = value ? 'Yes' : 'No';
                        } else if (value === null || value === undefined) {
                            displayValue = 'N/A';
                        }

                        popupContent += `
                            <div style="margin-bottom: 8px; padding: 5px; background-color: #f8f9fa; border-radius: 4px;">
                                <strong style="color: #495057; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">${key}</strong>
                                <div style="color: #212529; font-size: 14px; margin-top: 2px; word-break: break-word;">${displayValue}</div>
                            </div>
                        `;
                    });

                    popupContent += '</div>';
                } else {
                    popupContent += '<p style="color: #6c757d; font-style: italic;">No properties available for this feature.</p>';
                }

                // Add source layer information
                popupContent += `
                    <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #dee2e6; font-size: 12px; color: #6c757d;">
                        <strong>Source Layer:</strong> ${config.sourceLayer || 'Unknown'}
                    </div>
                `;

                popupContent += '</div>';

                // Set the popup content and show it
                popup.setHTML(popupContent);
                popup.setLngLat(coordinates).addTo(map);

                console.log('Clicked feature:', feature);
                console.log('Feature properties:', feature.properties);
            });

            // Change cursor to pointer when hovering over PMTiles objects
            map.on('mouseenter', 'geometries', () => {
                map.getCanvas().style.cursor = 'pointer';
            });

            map.on('mouseleave', 'geometries', () => {
                map.getCanvas().style.cursor = '';
            });

        }).catch(error => {
            console.error('Error loading PMTiles header:', error);
        });
    } catch (error) {
        console.error('Error fetching configuration:', error);
    }
}


// Initialize the map when the page loads
document.addEventListener('DOMContentLoaded', initializeMap); 