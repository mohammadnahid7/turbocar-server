const http = require('http');

const PORT = 3339;

// Helper to parse JSON or multipart/form-data body
const parseBody = (req) => {
    return new Promise((resolve) => {
        const chunks = [];
        req.on('data', chunk => {
            chunks.push(chunk);
        });
        req.on('end', () => {
            const buffer = Buffer.concat(chunks);
            const contentType = req.headers['content-type'] || '';

            // Handle multipart/form-data
            if (contentType.includes('multipart/form-data')) {
                const body = buffer.toString();
                const result = {};
                // Extract boundary from content-type
                const boundaryMatch = contentType.match(/boundary=(.+)/);
                if (boundaryMatch) {
                    const boundary = boundaryMatch[1];
                    const parts = body.split('--' + boundary).filter(p => p.trim() && p.trim() !== '--');
                    for (const part of parts) {
                        const nameMatch = part.match(/name="([^"]+)"/);
                        if (nameMatch) {
                            const name = nameMatch[1];
                            // Check if this is a file field
                            const filenameMatch = part.match(/filename="([^"]+)"/);
                            if (filenameMatch) {
                                result[name] = '[FILE: ' + filenameMatch[1] + ']';
                            } else {
                                // Text field: value is after the double newline
                                const valueMatch = part.split('\r\n\r\n');
                                if (valueMatch.length > 1) {
                                    result[name] = valueMatch.slice(1).join('\r\n\r\n').replace(/\r\n$/, '');
                                }
                            }
                        }
                    }
                }
                resolve(result);
            } else {
                // Handle JSON
                try {
                    resolve(JSON.parse(buffer.toString() || '{}'));
                } catch (e) {
                    resolve({});
                }
            }
        });
    });
};

// Response helper
const sendResponse = (res, statusCode, data) => {
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
};

const server = http.createServer(async (req, res) => {
    console.log(`\n[${new Date().toISOString()}] ${req.method} ${req.url}`);

    // Set CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    try {
        const body = await parseBody(req);
        if (Object.keys(body).length > 0) {
            console.log('Received Payload:', JSON.stringify(body, null, 2));
        }

        // --- ROUTES ---

        // Login Route
        if (req.url === '/auth/login' && req.method === 'POST') {
            const { user_email, user_password } = body;

            if (!user_email || !user_password) {
                console.log('❌ Login Failed: Missing email or password');
                return sendResponse(res, 400, { message: 'Missing credentials' });
            }

            console.log(`✅ Login Success for: ${user_email}`);
            return sendResponse(res, 200, {
                "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE3NzEzMTg4MTEsImV4cCI6MTgwMjg1NDgxMSwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsIkdpdmVuTmFtZSI6IkpvaG5ueSIsIkVtYWlsIjoianJvY2tldEBleGFtcGxlLmNvbSJ9.jGpSIbePUbn3_ixvNqa-e0fAy1HrC5UtqU3uvQYjivQ",
                "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE3NzEzMTg4MTEsImV4cCI6MTgwMjg1NDgxMSwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsIkdpdmVuTmFtZSI6IkpvaG5ueSIsIkVtYWlsIjoianJvY2tldEBleGFtcGxlLmNvbSJ9.jGpSIbePUbn3_ixvNqa-e0fAy1HrC5UtqU3uvQYjivQ",
                "token_expiry": new Date(Date.now() + 3600000).toISOString(),
                "user": {
                    "user_id": "user_123",
                    "user_firstname": "Test",
                    "user_lastname": "User",
                    "user_phone": "1234567890",
                    "email": user_email,
                    "user_role": "buyer",
                    "user_is_verified": true,
                    "user_created_at": new Date().toISOString(),
                    "user_updated_at": new Date().toISOString(),
                    "user_dob": "1990-01-01",
                    "user_gender": "male",
                }
            });
        }

        // Register Route
        if (req.url === '/auth/register' && req.method === 'POST') {
            const { email } = body;

            console.log(`✅ Register Success for: ${email}`);
            return sendResponse(res, 201, {
                "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE3NzEzMTg4MTEsImV4cCI6MTgwMjg1NDgxMSwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsIkdpdmVuTmFtZSI6IkpvaG5ueSIsIkVtYWlsIjoianJvY2tldEBleGFtcGxlLmNvbSJ9.jGpSIbePUbn3_ixvNqa-e0fAy1HrC5UtqU3uvQYjivQ",
                "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE3NzEzMTg4MTEsImV4cCI6MTgwMjg1NDgxMSwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoianJvY2tldEBleGFtcGxlLmNvbSIsIkdpdmVuTmFtZSI6IkpvaG5ueSIsIkVtYWlsIjoianJvY2tldEBleGFtcGxlLmNvbSJ9.jGpSIbePUbn3_ixvNqa-e0fAy1HrC5UtqU3uvQYjivQ",
                "token_expiry": new Date(Date.now() + 3600000).toISOString(), // 1 hour from now
                "user": {
                    "user_id": "user_123",
                    "user_firstname": "Test",
                    "user_lastname": "User",
                    "user_phone": "1234567890",
                    "email": email,
                    "user_role": "buyer",
                    "user_is_verified": true,
                    "user_created_at": new Date().toISOString(),
                    "user_updated_at": new Date().toISOString(),
                    "user_dob": "1990-01-01",
                    "user_gender": "male",
                }
            });
        }

        // Auth Verify (Me) Route
        if (req.url === '/auth/verify' || req.url === '/auth/me') {
            const authHeader = req.headers['authorization'];
            console.log('Headers:', req.headers);

            if (!authHeader) {
                console.log('❌ Verify Failed: No Authorization header');
                return sendResponse(res, 401, { message: 'Unauthorized' });
            }

            console.log('✅ Verify/Me Success');
            return sendResponse(res, 200, {
                "user": {
                    "user_id": "user_123",
                    "user_name": "Verified User",
                    "email": "verified@example.com",
                    "role": "buyer",
                    "is_verified": true,
                    "user_created_at": new Date().toISOString(),
                    "has_completed_onboarding": true,
                    "is_email_verified": true
                }
            });
        }


        // Google Auth Routes
        const url = new URL(req.url, `http://${req.headers.host}`);
        const pathname = url.pathname;

        if (pathname === '/auth/google/check') {
            const googleId = url.searchParams.get('google_id');
            console.log(`Checking Google ID: ${googleId}`);
            console.log('Headers:', url.headers);

            // Simulating a database check
            // For testing, let's say if the ID matches the one we just registered (stored in memory?)
            // Or just mock it based on a "known" ID or the query param.

            // To make this stateful without a real DB, let's use a global variable.
            // (Note: `users` needs to be defined globally outside the server callback if not already)

            const existingUser = global.mockUsers ? global.mockUsers[googleId] : null;

            console.log('⚠️ Google Check: User Not Found');
            return sendResponse(res, 404, { message: 'User not found' });
            if (existingUser) {
                console.log('✅ Google Check Success: User Found');
                return sendResponse(res, 200, {
                    "exists": true,
                    "access_token": "mock_access_token_" + googleId,
                    "refresh_token": "mock_refresh_token_" + googleId,
                    "expires_at": new Date(Date.now() + 3600000).toISOString(),
                    "user": existingUser
                });
            } else {
            }
        }

        if (pathname === '/auth/google/register' && req.method === 'POST') {
            const { google_id, first_name, last_name, role } = body;
            // Dart client sends 'user_email', generic client might send 'email'
            const email = body.user_email || body.email;

            console.log(`✅ Google Register Success for: ${email}`);
            console.log('Body:', body);

            // Save to mock store
            if (!global.mockUsers) global.mockUsers = {};

            const userId = google_id || body.user_google_id;

            global.mockUsers[userId] = {
                "user_id": "user_" + (userId || Date.now()),
                "user_firstname": first_name || body.user_firstname || "Test",
                "user_lastname": last_name || body.user_lastname || "User",
                "user_email": email, // Must match AppFields.userEmail
                "user_role": role || "buyer",
                "user_avatar_url": "https://upload.wikimedia.org/wikipedia/commons/b/b6/Image_created_with_a_mobile_phone.png",
                "user_is_verified": true,
                "user_created_at": new Date().toISOString(),
                "user_updated_at": new Date().toISOString(),
                "user_phone": body.user_phone
            };

            return sendResponse(res, 200, {
                "access_token": "mock_access_token_" + userId,
                "refresh_token": "mock_refresh_token_" + userId,
                "expires_at": new Date(Date.now() + 3600000).toISOString(),
                "user": global.mockUsers[userId]
            });
        }

        // Profile Routes
        if (req.url === '/profile' && req.method === 'GET') {
            console.log('✅ Profile API Hit (GET)');
            // Create a mock profile with firstName and lastName properly matching AppFields
            return sendResponse(res, 200, {
                "profile": {
                    "id": "user_123",
                    "user_firstname": "John",
                    "user_lastname": "Doe",
                    "user_phone": "+1234567890",
                    "user_email": "johndoe@example.com",
                    "user_avatar_url": "https://upload.wikimedia.org/wikipedia/commons/b/b6/Image_created_with_a_mobile_phone.png",
                    "user_role": "buyer",
                    "user_is_verified": true,
                    "phone_verified": true,
                    "user_created_at": new Date().toISOString(),
                    "user_updated_at": new Date().toISOString(),
                    "user_dob": "1990-01-01",
                    "user_gender": "Male"
                }
            });
        }

        if (req.url === '/profile' && req.method === 'PUT') {
            console.log('✅ Profile API Hit (PUT Update)');
            return sendResponse(res, 200, {
                "profile": {
                    "id": "user_123",
                    ...body,
                    "user_updated_at": new Date().toISOString(),
                }
            });
        }
        if (pathname.startsWith('/cars')) {
            try {
                // Delete module cache to always return fresh data if file changes
                delete require.cache[require.resolve('./cars.json')];
                const allCars = require('./cars.json');

                if (req.method === 'GET') {
                    if (pathname === '/cars') {
                        console.log('✅ Cars API Hit (List)');
                        const page = parseInt(url.searchParams.get('page')) || 1;
                        const limit = parseInt(url.searchParams.get('limit')) || 20;
                        const startIndex = (page - 1) * limit;
                        const endIndex = page * limit;

                        const paginatedCars = allCars.slice(startIndex, endIndex)

                        return sendResponse(res, 200, {
                            cars: paginatedCars,
                            total: allCars.length,
                            page, limit
                        });
                    }

                    if (pathname === '/cars/featured') {
                        console.log('✅ Cars API Hit (Featured)');
                        const limit = parseInt(url.searchParams.get('limit')) || 10;
                        const featuredCars = allCars.filter(c => c.is_featured).slice(0, limit);
                        return sendResponse(res, 200, { cars: featuredCars });
                    }

                    if (pathname === '/cars/search') {
                        console.log('✅ Cars API Hit (Search)');
                        const query = (url.searchParams.get('q') || '').toLowerCase();
                        const page = parseInt(url.searchParams.get('page')) || 1;
                        const limit = parseInt(url.searchParams.get('limit')) || 20;

                        const matchedCars = allCars.filter(c =>
                            c.title.toLowerCase().includes(query) ||
                            c.brand.toLowerCase().includes(query) ||
                            c.model.toLowerCase().includes(query)
                        );
                        const startIndex = (page - 1) * limit;
                        const endIndex = page * limit;

                        return sendResponse(res, 200, {
                            cars: matchedCars.slice(startIndex, endIndex),
                            total: matchedCars.length
                        });
                    }

                    if (pathname.startsWith('/cars/brand/')) {
                        const brand = pathname.split('/')[3];
                        console.log(`✅ Cars API Hit (Brand: ${brand})`);
                        const page = parseInt(url.searchParams.get('page')) || 1;
                        const limit = parseInt(url.searchParams.get('limit')) || 20;

                        const matchedCars = allCars.filter(c => c.brand.toLowerCase() === decodeURIComponent(brand).toLowerCase());
                        const startIndex = (page - 1) * limit;
                        const endIndex = page * limit;

                        return sendResponse(res, 200, { cars: matchedCars.slice(startIndex, endIndex) });
                    }

                    if (pathname.startsWith('/cars/seller/')) {
                        const sellerId = pathname.split('/')[3];
                        console.log(`✅ Cars API Hit (Seller: ${sellerId})`);
                        const page = parseInt(url.searchParams.get('page')) || 1;
                        const limit = parseInt(url.searchParams.get('limit')) || 20;

                        const matchedCars = allCars.filter(c => c.seller_id === sellerId);
                        const startIndex = (page - 1) * limit;
                        const endIndex = page * limit;

                        return sendResponse(res, 200, { cars: matchedCars.slice(startIndex, endIndex) });
                    }

                    // Single car GET (/cars/:id)
                    if (pathname.match(/^\/cars\/[^\/]+$/)) {
                        const carId = pathname.split('/')[2];
                        if (carId !== 'featured' && carId !== 'search') {
                            console.log(`✅ Cars API Hit (Single: ${carId})`);
                            const car = allCars.find(c => c.id === carId);
                            if (car) {
                                return sendResponse(res, 200, { car: car });
                            } else {
                                return sendResponse(res, 404, { message: 'Car not found' });
                            }
                        }
                    }
                }

                if (req.method === 'POST' && pathname.match(/^\/cars\/[^\/]+\/view$/)) {
                    const carId = pathname.split('/')[2];
                    console.log(`✅ Cars API Hit (View Increment: ${carId})`);
                    return sendResponse(res, 200, { message: 'View count incremented' });
                }

                // If path starts with /cars but didn't match anything above, we still return just so it doesn't fall through to 404
                return sendResponse(res, 404, { message: 'Car route not found' });

            } catch (err) {
                console.error('Error in cars routes:', err);
                return sendResponse(res, 500, { message: 'Failed to process cars request' });
            }
        }

        if (req.url === '/') {
            console.log('✅ Home Page Success');
            return sendResponse(res, 200, "Welcome to the Test Server");
        }

        // 404
        console.log('⚠️ 404 Route Not Found');
        sendResponse(res, 404, { message: 'Route not found' });

    } catch (err) {
        console.error('SERVER ERROR:', err);
        sendResponse(res, 500, { message: 'Internal Server Error' });
    }
});

server.listen(PORT, () => {
    console.log(`\n🚀 Test Server running at http://localhost:${PORT}`);
    console.log('Waiting for requests...\n');
});
