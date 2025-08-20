const express = require('express');
const path = require('path');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

// Databricks OAuth configuration
const DATABRICKS_CLIENT_ID = process.env.DATABRICKS_CLIENT_ID;
const DATABRICKS_CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET;
const DATABRICKS_HOST = process.env.DATABRICKS_HOST;

// Global variable to store the access token
let accessToken = null;
let tokenExpiry = null;

// Function to fetch Databricks access token
async function fetchDatabricksToken() {
    if (!DATABRICKS_CLIENT_ID || !DATABRICKS_CLIENT_SECRET || !DATABRICKS_HOST) {
        console.error('Missing (some) required environment variables: DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, DATABRICKS_HOST');
        return null;
    }

    const TOKEN_URL = `https://${DATABRICKS_HOST}/oidc/v1/token`;
    const auth = Buffer.from(`${DATABRICKS_CLIENT_ID}:${DATABRICKS_CLIENT_SECRET}`).toString('base64');

    try {
        const response = await axios.post(
            TOKEN_URL,
            new URLSearchParams({
                grant_type: 'client_credentials',
                scope: 'all-apis'
            }),
            {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': `Basic ${auth}`
                }
            }
        );

        accessToken = response.data.access_token;
        // Set token expiry (assuming 1 hour if not provided)
        const expiresIn = response.data.expires_in || 3600;
        tokenExpiry = new Date(Date.now() + (expiresIn * 1000));
        
        console.log('Access Token fetched successfully');
        return accessToken;
    } catch (error) {
        console.error('Error fetching token:', error.response?.data || error.message);
        return null;
    }
}

// Function to get valid access token (refreshes if needed)
async function getValidAccessToken() {
    // Check if token is expired or will expire in the next 5 minutes
    if (!accessToken || !tokenExpiry || new Date() > new Date(tokenExpiry.getTime() - 5 * 60 * 1000)) {
        console.log('Token expired or expiring soon, fetching new token...');
        await fetchDatabricksToken();
    }
    return accessToken;
}

// Initialize token on startup
fetchDatabricksToken().then(() => {
    console.log('Initial Databricks token fetch completed');
});

// Serve static files from the public directory
app.use(express.static('public'));

// Serve the src directory for JavaScript files
app.use('/src', express.static('src'));

// Proxy endpoint for PMTiles files with range request support
app.get('/proxy/pmtiles', async (req, res) => {
    let url = req.query.url;
    
    console.log('Proxy request received for URL:', url);
    
    if (!url) {
        return res.status(400).json({ error: 'URL parameter required' });
    }
    
    // Decode the URL if it's encoded
    try {
        url = decodeURIComponent(url);
        console.log('Decoded URL:', url);
    } catch (error) {
        console.log('URL was not encoded, using as-is');
    }
    
    // Check if this is a TileJSON request (no range header, likely from MapLibre)
    const isTileJsonRequest = !req.headers.range && req.headers.accept && req.headers.accept.includes('application/json');
    
    if (isTileJsonRequest) {
        // Return TileJSON metadata for MapLibre
        res.json({
            type: 'vector',
            tiles: [`${req.protocol}://${req.get('host')}/proxy/pmtiles?url=${url}`],
            minzoom: 0,
            maxzoom: 14,
        });
        return;
    }
    
    try {
        // Get the range header from the client request
        const range = req.headers.range;
        
        // Prepare headers for the request
        const headers = {
            'User-Agent': 'PMTiles-Viewer/1.0'
        };
        
        // Get valid access token and add authorization header
        const authToken = await getValidAccessToken();
        if (authToken) {
            headers['Authorization'] = `Bearer ${authToken}`;
            console.log('Using Databricks auth token for PMTiles request');
        } else {
            console.log('No auth token available for PMTiles request');
        }
        
        // Forward the range header if present
        if (range) {
            headers['Range'] = range;
        }
        
        console.log('Fetching URL with headers:', { url, headers: { ...headers, Authorization: headers.Authorization ? 'Bearer [REDACTED]' : undefined } });
        
        const response = await fetch(url, { headers });
        
        if (!response.ok) {
            console.error('HTTP error response:', response.status, response.statusText);
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        // Forward response headers
        res.set('Content-Type', 'application/octet-stream');
        res.set('Access-Control-Allow-Origin', '*');
        res.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
        res.set('Access-Control-Allow-Headers', 'Range, Authorization');
        
        // Forward range-related headers
        if (response.headers.get('content-range')) {
            res.set('Content-Range', response.headers.get('content-range'));
        }
        if (response.headers.get('content-length')) {
            res.set('Content-Length', response.headers.get('content-length'));
        }
        if (response.headers.get('accept-ranges')) {
            res.set('Accept-Ranges', response.headers.get('accept-ranges'));
        }
        
        // Set the appropriate status code
        if (response.status === 206) {
            res.status(206);
        } else {
            res.status(200);
        }
        
        // Stream the response
        const reader = response.body.getReader();
        const pump = async () => {
            try {
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    res.write(value);
                }
                res.end();
            } catch (error) {
                console.error('Streaming error:', error);
                res.status(500).end();
            }
        };
        
        pump();
        
    } catch (error) {
        console.error('Proxy error:', error);
        res.status(500).json({ error: 'Failed to fetch PMTiles file' });
    }
});

// Handle OPTIONS requests for CORS preflight
app.options('/proxy/pmtiles', (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Range, Authorization');
    res.sendStatus(200);
});

// API endpoint to get PMTiles configuration
app.get('/api/config', async (req, res) => {
    
    // Get filePath and sourceLayer from query parameters or use defaults
    const filePath = req.query.filePath;
    const sourceLayer = req.query.sourceLayer;
    
    console.log('Received parameters:', { filePath, sourceLayer });
        
    const ucUrl = `https://${DATABRICKS_HOST}/api/2.0/fs/files${filePath}`;
    
    console.log('Generated URL:', ucUrl);
    
    // Create a proxy URL that includes the auth token
    const proxyUrl = `/proxy/pmtiles?url=${encodeURIComponent(ucUrl)}`;
    
    res.json({
        pmtilesUrl: proxyUrl,
        sourceLayer: sourceLayer,
        baseMapUrl: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
        zoom: 7
    });
});

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Serve the main page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start the server
app.listen(PORT, () => {
    console.log(`PMTiles Viewer server running on http://localhost:${PORT}`);
    console.log(`API available at http://localhost:${PORT}/api/config`);
}); 