// ============================================================================
//  TurboCar Server — Express + PostgreSQL (Supabase)
//  All routes in one file, organised by section.
//  Drop-in replacement for the file-based JSON DB version.
//  Run: npm install && npm start
// ============================================================================

require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const os = require('os');
const http = require('http');
const { WebSocketServer } = require('ws');
const url = require('url');
const jwt = require('jsonwebtoken');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3339;

// ─── Middleware ──────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Multer for multipart/form-data (profile avatar uploads, etc.)
const upload = multer({ dest: path.join(os.tmpdir(), 'turbocar-uploads') });

// Request logger
app.use((req, res, next) => {
    console.log(`\n[${new Date().toISOString()}] ${req.method} ${req.url}`);
    if (req.body && Object.keys(req.body).length > 0) {
        console.log('  Payload:', JSON.stringify(req.body, null, 2));
    }
    next();
});

// ─── PostgreSQL Connection Pool ──────────────────────────────────────────────
// Set DATABASE_URL in your .env (Supabase → Project Settings → Database → URI)
// For Supabase, always enable SSL.  rejectUnauthorized: false is safe for Render.
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

pool.on('error', (err) => {
    console.error('❌ Unexpected PostgreSQL pool error:', err.message);
});

// Verify connection at startup — fail fast if misconfigured.
pool.query('SELECT NOW()')
    .then(r => console.log(`✅ PostgreSQL connected (${r.rows[0].now})`))
    .catch(err => {
        console.error('❌ FATAL: Cannot connect to PostgreSQL:', err.message);
        process.exit(1);
    });

// ─── JWT Configuration ──────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET;
const JWT_ACCESS_EXPIRY = process.env.JWT_ACCESS_EXPIRY || '1h';
const JWT_REFRESH_EXPIRY = process.env.JWT_REFRESH_EXPIRY || '7d';

if (!JWT_SECRET) {
    console.error('❌ FATAL: JWT_SECRET is not set in .env — server cannot start securely.');
    process.exit(1);
}
console.log('🔐 JWT configured (access: ' + JWT_ACCESS_EXPIRY + ', refresh: ' + JWT_REFRESH_EXPIRY + ')');

// ─── Cloudflare R2 Configuration ────────────────────────────────────────────
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || '';
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || '';
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || '';
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || 'turbocar';
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL || '';

const r2Enabled = R2_ACCOUNT_ID && R2_ACCESS_KEY_ID && R2_SECRET_ACCESS_KEY && R2_PUBLIC_URL;

let s3Client = null;
if (r2Enabled) {
    s3Client = new S3Client({
        region: 'auto',
        endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: R2_ACCESS_KEY_ID,
            secretAccessKey: R2_SECRET_ACCESS_KEY,
        },
    });
    console.log('☁️  Cloudflare R2 configured successfully');
} else {
    console.log('⚠️  R2 credentials not configured — falling back to local file storage');
}

/**
 * Upload a file to Cloudflare R2 (or fall back to local URL).
 *
 * @param {object} file          - Multer file object
 * @param {string} folderPrefix  - e.g. 'users/' or 'cars/'
 * @returns {Promise<string>}    Public URL of the uploaded file
 */
async function uploadToR2(file, folderPrefix = '') {
    if (!r2Enabled || !s3Client) {
        return `http://localhost:${PORT}/uploads/${file.filename}`;
    }

    const ext = path.extname(file.originalname || '').toLowerCase() || '.jpg';
    const uniqueKey = `${folderPrefix}${Date.now()}_${Math.random().toString(36).substring(2, 8)}${ext}`;
    const fileBuffer = fs.readFileSync(file.path);

    const command = new PutObjectCommand({
        Bucket: R2_BUCKET_NAME,
        Key: uniqueKey,
        Body: fileBuffer,
        ContentType: file.mimetype || 'image/jpeg',
    });

    await s3Client.send(command);
    console.log(`  ☁️  Uploaded to R2: ${uniqueKey}`);

    fs.unlink(file.path, (err) => {
        if (err) console.warn(`  ⚠️  Failed to delete temp file: ${file.path}`);
    });

    return `${R2_PUBLIC_URL}/${uniqueKey}`;
}

// ─── Helper: generate real JWT tokens ───────────────────────────────────────
function generateTokens(userId) {
    const access_token = jwt.sign({ sub: userId, type: 'access' }, JWT_SECRET, {
        expiresIn: JWT_ACCESS_EXPIRY,
    });
    const refresh_token = jwt.sign({ sub: userId, type: 'refresh' }, JWT_SECRET, {
        expiresIn: JWT_REFRESH_EXPIRY,
    });
    const decoded = jwt.decode(access_token);
    return {
        access_token,
        refresh_token,
        token_expiry: new Date(decoded.exp * 1000).toISOString(),
    };
}

// ─── Helper: extract userId from JWT (verify signature + expiry) ─────────────
function extractUserId(req) {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return null;
    try {
        const token = authHeader.replace('Bearer ', '');
        const payload = jwt.verify(token, JWT_SECRET);
        return payload.sub;
    } catch (e) {
        return null;
    }
}

// ─── Middleware: authenticate token for protected routes ──────────────────────
function authenticateToken(req, res, next) {
    const userId = extractUserId(req);
    if (!userId) {
        return res.status(401).json({ message: 'Unauthorized: Invalid or expired token' });
    }
    req.userId = userId;
    next();
}

// ─── Unified Model Responses ─────────────────────────────────────────────────

// 1. Auth Response (UserModel fields ONLY)
function buildUserResponse(user) {
    return {
        user_id: user.user_id,
        user_firstname: user.user_firstname,
        user_lastname: user.user_lastname,
        user_phone: user.user_phone || null,
        user_email: user.user_email,
        user_avatar_url: user.user_avatar_url || null,
        user_role: user.user_role || 'buyer',
        user_is_verified: user.user_is_verified !== undefined ? user.user_is_verified : true,
        user_created_at: user.user_created_at || new Date().toISOString(),
        user_updated_at: user.user_updated_at || new Date().toISOString(),
    };
}

// 2. Profile Response (UserProfileModel fields ONLY)
function buildProfileResponse(user) {
    return {
        id: user.user_id,
        user_firstname: user.user_firstname,
        user_lastname: user.user_lastname,
        user_phone: user.user_phone || null,
        user_email: user.user_email,
        user_avatar_url: user.user_avatar_url || null,
        address: user.address || null,
        city: user.city || null,
        state: user.state || null,
        postal_code: user.postal_code || null,
        country: user.country || null,
        user_bio: user.user_bio || null,
        user_dob: user.user_dob || null,
        user_gender: user.user_gender || null,
        user_role: user.user_role || 'buyer',
        user_is_verified: user.user_is_verified !== undefined ? user.user_is_verified : true,
        phone_verified: user.phone_verified !== undefined ? user.phone_verified : false,
        user_created_at: user.user_created_at || new Date().toISOString(),
        user_updated_at: user.user_updated_at || new Date().toISOString(),
        preferred_language: user.preferred_language || null,
        timezone: user.timezone || null,
    };
}

// 3. Car Response (Ensure types are correct for Dart/Flutter)
function formatCarResponse(car) {
    if (!car) return car;
    return {
        ...car,
        price: car.price != null ? parseFloat(car.price) : 0,
        price_in_cents: car.price_in_cents != null ? parseInt(car.price_in_cents, 10) : 0,
        mileage: car.mileage != null ? parseInt(car.mileage, 10) : 0,
        mileage_km: car.mileage_km != null ? parseInt(car.mileage_km, 10) : 0,
        year: car.year != null ? parseInt(car.year, 10) : 0,
        seats: car.seats != null ? parseInt(car.seats, 10) : 0,
        view_count: car.view_count != null ? parseInt(car.view_count, 10) : 0,
        previous_owners: car.previous_owners != null ? parseInt(car.previous_owners, 10) : 0,
    };
}

// ─── Helper: build a safe dynamic SET clause for UPDATE queries ───────────────
// Only fields present in `allowedFields` are included — prevents arbitrary injection.
// Returns { text: "field1 = $1, field2 = $2", values: [...], nextIndex: N }
function buildSetClause(updates, allowedFields, startIndex = 1) {
    const parts = [];
    const values = [];
    let idx = startIndex;

    for (const field of allowedFields) {
        if (updates[field] !== undefined) {
            parts.push(`"${field}" = $${idx++}`);
            values.push(updates[field]);
        }
    }

    return { text: parts.join(', '), values, nextIndex: idx };
}


// ═══════════════════════════════════════════════════════════════════════════
//  ROUTES
// ═══════════════════════════════════════════════════════════════════════════


// ─── Auth Routes ─────────────────────────────────────────────────────────────

app.post('/auth/login', async (req, res) => {
    const { user_email, user_password } = req.body;

    if (!user_email || !user_password) {
        return res.status(400).json({ message: 'Missing credentials' });
    }

    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE user_email = $1 LIMIT 1',
            [user_email]
        );

        if (result.rowCount === 0) {
            return res.status(404).json({ message: 'User not found. Please register first.' });
        }

        const user = result.rows[0];
        console.log(`✅ Login Success for: ${user_email}`);
        const tokens = generateTokens(user.user_id);
        return res.status(200).json({ ...tokens, user: buildUserResponse(user) });
    } catch (err) {
        console.error('❌ Login error:', err);
        return res.status(500).json({ message: 'Login failed: ' + err.message });
    }
});

app.post('/auth/register', upload.single('user_avatar_url'), async (req, res) => {
    try {
        const {
            user_email, email,
            user_firstname, user_lastname,
            user_phone, user_role,
            user_dob, user_gender,
        } = req.body;
        const finalEmail = user_email || email;

        // Return existing user if already registered (idempotent)
        const existing = await pool.query(
            'SELECT * FROM users WHERE user_email = $1 LIMIT 1',
            [finalEmail]
        );
        if (existing.rowCount > 0) {
            const tokens = generateTokens(existing.rows[0].user_id);
            return res.status(200).json({ ...tokens, user: buildUserResponse(existing.rows[0]) });
        }

        let avatarUrl = null;
        if (req.file) {
            avatarUrl = await uploadToR2(req.file, 'users/');
            console.log(`  📸 Avatar uploaded: ${req.file.originalname}`);
        }

        const userId = 'user_' + Date.now();
        const now = new Date().toISOString();

        const insert = await pool.query(
            `INSERT INTO users
               (user_id, user_firstname, user_lastname, user_phone, user_email,
                user_avatar_url, user_dob, user_gender, user_role,
                user_is_verified, user_created_at, user_updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
             RETURNING *`,
            [
                userId,
                user_firstname || 'Test',
                user_lastname || 'User',
                user_phone || null,
                finalEmail,
                avatarUrl,
                user_dob || null,
                user_gender || null,
                user_role || 'buyer',
                true,
                now,
                now,
            ]
        );

        const newUser = insert.rows[0];
        console.log(`✅ Register Success for: ${finalEmail}`);
        const tokens = generateTokens(newUser.user_id);
        return res.status(201).json({ ...tokens, user: buildUserResponse(newUser) });
    } catch (err) {
        console.error('❌ Register error:', err);
        return res.status(500).json({ message: 'Registration failed: ' + err.message });
    }
});

app.get(['/auth/verify', '/auth/me'], authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE user_id = $1 LIMIT 1',
            [req.userId]
        );
        if (result.rowCount === 0) return res.status(404).json({ message: 'User not found' });

        console.log('✅ Verify/Me Success');
        return res.status(200).json({ user: buildUserResponse(result.rows[0]) });
    } catch (err) {
        console.error('❌ Verify error:', err);
        return res.status(500).json({ message: 'Verification failed: ' + err.message });
    }
});

// POST /auth/refresh — issue new token pair from a valid refresh token
app.post('/auth/refresh', async (req, res) => {
    const { refresh_token } = req.body;
    if (!refresh_token) {
        return res.status(400).json({ message: 'refresh_token is required' });
    }

    try {
        const payload = jwt.verify(refresh_token, JWT_SECRET);
        if (payload.type !== 'refresh') {
            return res.status(401).json({ message: 'Invalid token type' });
        }

        const result = await pool.query(
            'SELECT * FROM users WHERE user_id = $1 LIMIT 1',
            [payload.sub]
        );
        if (result.rowCount === 0) {
            return res.status(404).json({ message: 'User not found' });
        }

        const user = result.rows[0];
        const tokens = generateTokens(user.user_id);
        console.log(`✅ Token Refreshed for: ${user.user_email}`);
        return res.status(200).json({ ...tokens, user: buildUserResponse(user) });
    } catch (e) {
        return res.status(401).json({ message: 'Invalid or expired refresh token' });
    }
});

// ─── Google Auth Routes ───────────────────────────────────────────────────────

app.get('/auth/google/check', async (req, res) => {
    const googleId = 'user_' + req.query.google_id;
    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE user_id = $1 LIMIT 1',
            [googleId]
        );
        console.log('Existing User check:', googleId, 'found:', result.rowCount > 0);

        if (result.rowCount > 0) {
            const tokens = generateTokens(result.rows[0].user_id);
            return res.status(200).json({
                exists: true,
                ...tokens,
                user: buildUserResponse(result.rows[0]),
            });
        }

        return res.status(404).json({ message: 'User not found' });
    } catch (err) {
        console.error('❌ Google check error:', err);
        return res.status(500).json({ message: 'Google check failed: ' + err.message });
    }
});

app.post('/auth/google/register', upload.single('user_avatar_url'), async (req, res) => {
    try {
        const {
            user_google_id,
            user_email, email,
            first_name, user_firstname,
            last_name, user_lastname,
            user_phone, user_dob, user_gender, role,
        } = req.body;

        console.log('Payload:', req.body);

        const finalEmail = user_email || email;
        const finalGoogleId = user_google_id;

        let avatarUrl = null;
        if (req.file) {
            avatarUrl = await uploadToR2(req.file, 'users/');
            console.log(`  📸 Google avatar uploaded: ${req.file.originalname}`);
        } else {
            avatarUrl = req.body.user_avatar_url || null;
        }

        const userId = 'user_' + (finalGoogleId || Date.now());
        const now = new Date().toISOString();

        // Upsert: insert if not exists, otherwise update mutable profile fields.
        const result = await pool.query(
            `INSERT INTO users
               (user_id, user_firstname, user_lastname, user_phone, user_email,
                user_dob, user_gender, user_role, user_avatar_url,
                user_is_verified, user_created_at, user_updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
             ON CONFLICT (user_id) DO UPDATE SET
               user_firstname  = EXCLUDED.user_firstname,
               user_lastname   = EXCLUDED.user_lastname,
               user_avatar_url = EXCLUDED.user_avatar_url,
               user_updated_at = EXCLUDED.user_updated_at
             RETURNING *`,
            [
                userId,
                user_firstname || first_name || 'Google',
                user_lastname || last_name || 'User',
                user_phone || null,
                finalEmail,
                user_dob || null,
                user_gender || null,
                role || 'buyer',
                avatarUrl,
                true,
                now,
                now,
            ]
        );

        const newUser = result.rows[0];
        console.log(`✅ Google Register Success for: ${finalEmail}`);
        const tokens = generateTokens(newUser.user_id);
        return res.status(201).json({ ...tokens, user: buildUserResponse(newUser) });
    } catch (err) {
        console.error('❌ Google Register error:', err);
        return res.status(500).json({ message: 'Google registration failed: ' + err.message });
    }
});


// ─── Profile Routes ───────────────────────────────────────────────────────────

app.get('/profile', authenticateToken, async (req, res) => {
    console.log('✅ Profile API Hit (GET)');
    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE user_id = $1 LIMIT 1',
            [req.userId]
        );
        if (result.rowCount === 0) return res.status(404).json({ message: 'User not found' });

        return res.status(200).json({ profile: buildProfileResponse(result.rows[0]) });
    } catch (err) {
        console.error('❌ Profile GET error:', err);
        return res.status(500).json({ message: 'Failed to fetch profile: ' + err.message });
    }
});

// Whitelist of updatable profile columns (prevents arbitrary column injection)
const PROFILE_UPDATABLE_FIELDS = [
    'user_firstname', 'user_lastname', 'user_phone', 'user_dob', 'user_gender',
    'user_bio', 'address', 'city', 'state', 'postal_code', 'country',
    'preferred_language', 'timezone', 'user_avatar_url',
];

app.put('/profile', authenticateToken, upload.single('avatar'), async (req, res) => {
    console.log('✅ Profile API Hit (PUT Update)');
    try {
        const updates = { ...req.body };

        if (req.file) {
            updates.user_avatar_url = await uploadToR2(req.file, 'users/');
            console.log(`  📸 Avatar uploaded: ${req.file.originalname}`);
        }

        // Build safe dynamic SET clause
        const { text: setClauses, values, nextIndex } = buildSetClause(updates, PROFILE_UPDATABLE_FIELDS, 1);

        if (!setClauses) {
            return res.status(400).json({ message: 'No valid fields to update' });
        }

        // Always bump user_updated_at
        const query = `
            UPDATE users
            SET ${setClauses}, user_updated_at = $${nextIndex}
            WHERE user_id = $${nextIndex + 1}
            RETURNING *
        `;
        values.push(new Date().toISOString(), req.userId);

        const result = await pool.query(query, values);
        if (result.rowCount === 0) return res.status(404).json({ message: 'User not found' });

        return res.status(200).json({ profile: buildProfileResponse(result.rows[0]) });
    } catch (err) {
        console.error('❌ Profile update error:', err);
        return res.status(500).json({ message: 'Profile update failed: ' + err.message });
    }
});

// POST /profile/photo — standalone profile photo upload
app.post('/profile/photo', upload.single('photo'), async (req, res) => {
    console.log('✅ Profile Photo API Hit (POST Upload)');
    try {
        const userId = extractUserId(req);
        if (!userId) return res.status(401).json({ message: 'Unauthorized' });

        if (!req.file) return res.status(400).json({ message: 'No photo file provided' });

        const photoUrl = await uploadToR2(req.file, 'users/');
        console.log(`  📸 Profile photo uploaded: ${req.file.originalname}`);

        const result = await pool.query(
            'UPDATE users SET user_avatar_url = $1, user_updated_at = $2 WHERE user_id = $3 RETURNING *',
            [photoUrl, new Date().toISOString(), userId]
        );
        if (result.rowCount === 0) return res.status(404).json({ message: 'User not found' });

        return res.status(200).json({ photo_url: photoUrl });
    } catch (err) {
        console.error('❌ Profile photo upload error:', err);
        return res.status(500).json({ message: 'Photo upload failed: ' + err.message });
    }
});

app.use('/uploads', express.static(path.join(__dirname, 'uploads')));


// ─── Cars Routes ──────────────────────────────────────────────────────────────

// Helper: one-liner to add `is_favorited` via LEFT JOIN.
// When userId is null the join condition never matches → is_favorited = false.
const CAR_FAVORITE_SELECT = (userId) => `
    SELECT c.*,
           CASE WHEN f.car_id IS NOT NULL THEN true ELSE false END AS is_favorited
    FROM   cars c
    LEFT JOIN favorites f ON f.car_id = c.id AND f.user_id = ${userId ? `'${userId}'` : 'NULL'}
`;
// NOTE: userId is extracted from a verified JWT — not from user input — so
//       inlining it here is safe. We still use parameterised queries everywhere
//       else (brand, search, etc.) that accept user-provided strings.

// Parameterised version used when userId comes alongside other bound params.
const carFavJoin = `
    SELECT c.*,
           CASE WHEN f.car_id IS NOT NULL THEN true ELSE false END AS is_favorited
    FROM   cars c
    LEFT JOIN favorites f ON f.car_id = c.id AND f.user_id = $1
`;

app.get('/cars', async (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const userId = extractUserId(req);

    try {
        const [carsResult, countResult] = await Promise.all([
            pool.query(
                `${carFavJoin}
                 ORDER BY c.created_at DESC
                 LIMIT $2 OFFSET $3`,
                [userId, limit, offset]
            ),
            pool.query('SELECT COUNT(*) FROM cars'),
        ]);

        return res.status(200).json({
            cars: carsResult.rows.map(formatCarResponse),
            total: parseInt(countResult.rows[0].count),
            page,
            limit,
        });
    } catch (err) {
        console.error('❌ GET /cars error:', err);
        return res.status(500).json({ message: 'Failed to fetch cars: ' + err.message });
    }
});

app.post('/cars', upload.array('images', 10), async (req, res) => {
    console.log('✅ Car API Hit (POST Create)');
    try {
        // Extract seller_id from auth token
        let sellerId = 'unknown_seller';
        const authHeader = req.headers['authorization'];
        if (authHeader) {
            try {
                const tokenPart = authHeader.replace('Bearer ', '').split('.')[1];
                const payload = JSON.parse(Buffer.from(tokenPart, 'base64').toString());
                sellerId = payload.sub;
            } catch (e) { /* ignore */ }
        }

        const {
            car_type, brand, model, fuel_type,
            mileage, year, price, chat_only,
            city, description,
        } = req.body;

        // Upload images to R2
        const imageUrls = [];
        for (const file of (req.files || [])) {
            imageUrls.push(await uploadToR2(file, 'cars/'));
        }

        // Resolve seller name from DB
        let sellerName = 'Unknown Seller';
        const sellerResult = await pool.query(
            'SELECT user_firstname, user_lastname FROM users WHERE user_id = $1 LIMIT 1',
            [sellerId]
        );
        if (sellerResult.rowCount > 0) {
            const s = sellerResult.rows[0];
            sellerName = `${s.user_firstname} ${s.user_lastname}`.trim();
        }

        const carId = 'car_' + Date.now();
        const now = new Date().toISOString();
        const yearInt = parseInt(year) || 2024;
        const priceDec = parseFloat(price) || 0;
        const milesInt = parseInt(mileage) || 0;

        const result = await pool.query(
            `INSERT INTO cars
               (id, title, brand, model, year, price, price_in_cents,
                mileage, mileage_km, fuel_type, transmission, car_type, body_type,
                color, seats, image_urls, description, city, location,
                condition, seller_id, seller_name, is_available, is_featured,
                view_count, vin, engine_specs, previous_owners, features,
                chat_only, created_at, updated_at)
             VALUES
               ($1,$2,$3,$4,$5,$6,$7,
                $8,$9,$10,$11,$12,$13,
                $14,$15,$16,$17,$18,$19,
                $20,$21,$22,$23,$24,
                $25,$26,$27,$28,$29,
                $30,$31,$32)
             RETURNING *`,
            [
                carId,
                `${yearInt} ${brand || ''} ${model || ''}`.trim(),
                brand || 'Unknown Brand',
                model || 'Unknown Model',
                yearInt,
                priceDec,
                Math.round(priceDec * 100),
                milesInt,
                milesInt,
                fuel_type || 'Petrol',
                'Automatic',
                car_type || 'SUV',
                car_type || 'SUV',
                'Black',
                5,
                imageUrls,       // pg serialises JS arrays → TEXT[]
                description || '',
                city || 'Unknown City',
                city || 'Unknown City',
                'Used',
                sellerId,
                sellerName,
                true,
                false,
                0,
                'UNKNOWNVIN123456',
                'Standard Engine',
                1,
                ['Air Conditioning', 'Power Steering'],
                (chat_only === 'true' || chat_only === true),
                now,
                now,
            ]
        );

        const newCar = { ...result.rows[0], is_favorited: false };
        console.log(`✅ Car Created: ${newCar.title}`);
        return res.status(201).json(formatCarResponse(newCar));
    } catch (err) {
        console.error('❌ Car create error:', err);
        return res.status(500).json({ message: 'Car creation failed: ' + err.message });
    }
});

// Whitelist of updatable car columns
const CAR_UPDATABLE_FIELDS = [
    'brand', 'model', 'fuel_type', 'year', 'price', 'price_in_cents',
    'mileage', 'mileage_km', 'car_type', 'body_type', 'city', 'location',
    'description', 'title', 'image_urls', 'chat_only', 'is_available',
];

// PATCH /cars/:id — edit an existing listing
app.patch('/cars/:id', upload.array('new_images', 10), async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    console.log(`✅ Car API Hit (PATCH Update) id=${req.params.id}`);
    try {
        const carId = req.params.id;

        // Confirm the car exists first
        const existing = await pool.query('SELECT * FROM cars WHERE id = $1 LIMIT 1', [carId]);
        if (existing.rowCount === 0) return res.status(404).json({ message: 'Car not found' });

        const car = existing.rows[0];

        // Ownership check — only the seller can edit their listing
        if (car.seller_id !== userId) {
            return res.status(403).json({ message: 'You are not authorized to edit this listing' });
        }
        const {
            car_type, brand, model, fuel_type,
            mileage, year, price, chat_only,
            city, description, existing_image_urls,
        } = req.body;

        // Build update payload — normalise dual-field aliases
        const updates = {};

        if (car_type !== undefined) { updates.car_type = car_type; updates.body_type = car_type; }
        if (brand !== undefined) updates.brand = brand;
        if (model !== undefined) updates.model = model;
        if (fuel_type !== undefined) updates.fuel_type = fuel_type;
        if (mileage !== undefined) { updates.mileage = parseInt(mileage); updates.mileage_km = parseInt(mileage); }
        if (year !== undefined) updates.year = parseInt(year);
        if (price !== undefined) { updates.price = parseFloat(price); updates.price_in_cents = Math.round(parseFloat(price) * 100); }
        if (chat_only !== undefined) updates.chat_only = (chat_only === 'true' || chat_only === true);
        if (city !== undefined) { updates.city = city; updates.location = city; }
        if (description !== undefined) updates.description = description;

        // Re-derive title from merged brand/model/year
        const finalYear = updates.year || car.year || '';
        const finalBrand = updates.brand || car.brand || '';
        const finalModel = updates.model || car.model || '';
        updates.title = `${finalYear} ${finalBrand} ${finalModel}`.trim();

        // Merge retained + newly-uploaded image URLs
        let retainedUrls = [];
        if (existing_image_urls) {
            retainedUrls = Array.isArray(existing_image_urls)
                ? existing_image_urls
                : [existing_image_urls];
        }
        const newUrls = [];
        for (const file of (req.files || [])) {
            newUrls.push(await uploadToR2(file, 'cars/'));
        }
        updates.image_urls = [...retainedUrls, ...newUrls];

        const { text: setClauses, values, nextIndex } = buildSetClause(updates, CAR_UPDATABLE_FIELDS, 1);

        if (!setClauses) return res.status(400).json({ message: 'No valid fields to update' });

        const query = `
            UPDATE cars
            SET ${setClauses}, updated_at = $${nextIndex}
            WHERE id = $${nextIndex + 1}
            RETURNING *
        `;
        values.push(new Date().toISOString(), carId);

        const result = await pool.query(query, values);
        if (result.rowCount === 0) return res.status(500).json({ message: 'Failed to update car' });

        console.log(`✅ Car Updated: ${result.rows[0].title}`);
        return res.status(200).json(formatCarResponse(result.rows[0]));
    } catch (err) {
        console.error('❌ Car update error:', err);
        return res.status(500).json({ message: 'Car update failed: ' + err.message });
    }
});

// NOTE: Specific sub-routes (/featured, /search, /brand/:x, /seller/:x) must be
// registered BEFORE the generic /cars/:id route so Express matches them first.

app.get('/cars/featured', async (req, res) => {
    const limit = parseInt(req.query.limit) || 10;
    const userId = extractUserId(req);
    try {
        const result = await pool.query(
            `${carFavJoin}
             WHERE c.is_featured = true
             ORDER BY c.created_at DESC
             LIMIT $2`,
            [userId, limit]
        );
        return res.status(200).json({ cars: result.rows.map(formatCarResponse) });
    } catch (err) {
        console.error('❌ GET /cars/featured error:', err);
        return res.status(500).json({ message: 'Failed to fetch featured cars: ' + err.message });
    }
});

app.get('/cars/search', async (req, res) => {
    const query = (req.query.q || '').toLowerCase();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const userId = extractUserId(req);
    const like = `%${query}%`;

    try {
        const [carsResult, countResult] = await Promise.all([
            pool.query(
                `${carFavJoin}
                 WHERE lower(c.title) LIKE $2
                    OR lower(c.brand) LIKE $2
                    OR lower(c.model) LIKE $2
                 ORDER BY c.created_at DESC
                 LIMIT $3 OFFSET $4`,
                [userId, like, limit, offset]
            ),
            pool.query(
                `SELECT COUNT(*) FROM cars
                 WHERE lower(title) LIKE $1
                    OR lower(brand)  LIKE $1
                    OR lower(model)  LIKE $1`,
                [like]
            ),
        ]);

        return res.status(200).json({
            cars: carsResult.rows.map(formatCarResponse),
            total: parseInt(countResult.rows[0].count),
        });
    } catch (err) {
        console.error('❌ GET /cars/search error:', err);
        return res.status(500).json({ message: 'Search failed: ' + err.message });
    }
});

app.get('/cars/brand/:brand', async (req, res) => {
    const brand = decodeURIComponent(req.params.brand);
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const userId = extractUserId(req);

    try {
        const result = await pool.query(
            `${carFavJoin}
             WHERE lower(c.brand) = lower($2)
             ORDER BY c.created_at DESC
             LIMIT $3 OFFSET $4`,
            [userId, brand, limit, offset]
        );
        return res.status(200).json({ cars: result.rows.map(formatCarResponse) });
    } catch (err) {
        console.error('❌ GET /cars/brand error:', err);
        return res.status(500).json({ message: 'Failed to fetch cars by brand: ' + err.message });
    }
});

app.get('/cars/seller/:sellerId', async (req, res) => {
    const sellerId = req.params.sellerId;
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const userId = extractUserId(req);

    try {
        const result = await pool.query(
            `${carFavJoin}
             WHERE c.seller_id = $2
             ORDER BY c.created_at DESC
             LIMIT $3 OFFSET $4`,
            [userId, sellerId, limit, offset]
        );
        return res.status(200).json({ cars: result.rows.map(formatCarResponse) });
    } catch (err) {
        console.error('❌ GET /cars/seller error:', err);
        return res.status(500).json({ message: 'Failed to fetch seller cars: ' + err.message });
    }
});

app.get('/cars/:id', async (req, res) => {
    const userId = extractUserId(req);
    try {
        const result = await pool.query(
            `${carFavJoin}
             WHERE c.id = $2
             LIMIT 1`,
            [userId, req.params.id]
        );
        if (result.rowCount === 0) return res.status(404).json({ message: 'Car not found' });

        return res.status(200).json({ car: formatCarResponse(result.rows[0]) });
    } catch (err) {
        console.error('❌ GET /cars/:id error:', err);
        return res.status(500).json({ message: 'Failed to fetch car: ' + err.message });
    }
});

app.post('/cars/:id/view', async (req, res) => {
    try {
        await pool.query(
            'UPDATE cars SET view_count = view_count + 1 WHERE id = $1',
            [req.params.id]
        );
        return res.status(200).json({ message: 'View count incremented' });
    } catch (err) {
        console.error('❌ POST /cars/:id/view error:', err);
        return res.status(500).json({ message: 'Failed to increment view: ' + err.message });
    }
});

// DELETE /cars/:id — delete a car listing (owner only)
app.delete('/cars/:id', async (req, res) => {
    console.log(`✅ Car API Hit (DELETE) id=${req.params.id}`);
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const carId = req.params.id;
    try {
        // Verify ownership
        const existing = await pool.query('SELECT seller_id FROM cars WHERE id = $1 LIMIT 1', [carId]);
        if (existing.rowCount === 0) return res.status(404).json({ message: 'Car not found' });
        if (existing.rows[0].seller_id !== userId) {
            return res.status(403).json({ message: 'You can only delete your own listings' });
        }

        // Delete associated favorites first
        await pool.query('DELETE FROM favorites WHERE car_id = $1', [carId]);
        // Delete the car
        await pool.query('DELETE FROM cars WHERE id = $1', [carId]);

        console.log(`✅ Car Deleted: ${carId}`);
        return res.status(200).json({ message: 'Car deleted successfully' });
    } catch (err) {
        console.error('❌ DELETE /cars/:id error:', err);
        return res.status(500).json({ message: 'Failed to delete car: ' + err.message });
    }
});


// ═══════════════════════════════════════════════════════════════════════════
//  FAVORITES ROUTES
// ═══════════════════════════════════════════════════════════════════════════

// POST /favorites — add a car to favorites
app.post('/favorites', async (req, res) => {
    console.log('✅ Favorites API Hit (POST)');
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const { car_id } = req.body;
    if (!car_id) return res.status(400).json({ message: 'car_id is required' });

    try {
        // Detect duplicate before insert to return a meaningful 409
        const dup = await pool.query(
            'SELECT id FROM favorites WHERE user_id = $1 AND car_id = $2 LIMIT 1',
            [userId, car_id]
        );
        if (dup.rowCount > 0) {
            return res.status(409).json({ message: 'Car is already in favorites' });
        }

        const result = await pool.query(
            `INSERT INTO favorites (id, user_id, car_id, saved_at, is_synced, source)
             VALUES ($1, $2, $3, NOW(), true, 'user')
             RETURNING *`,
            ['fav_' + Date.now(), userId, car_id]
        );

        console.log(`✅ Favorite Added: car ${car_id} for user ${userId}`);
        return res.status(201).json({ favorite: result.rows[0] });
    } catch (err) {
        console.error('❌ POST /favorites error:', err);
        return res.status(500).json({ message: 'Failed to add favorite: ' + err.message });
    }
});

// DELETE /favorites/:carId — remove a car from favorites
app.delete('/favorites/:carId', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const carId = req.params.carId;
    try {
        const result = await pool.query(
            'DELETE FROM favorites WHERE user_id = $1 AND car_id = $2 RETURNING id',
            [userId, carId]
        );

        if (result.rowCount === 0) {
            return res.status(404).json({ message: 'Favorite not found' });
        }

        console.log(`✅ Favorite Removed: car ${carId} for user ${userId}`);
        return res.status(200).json({ message: 'Removed from favorites' });
    } catch (err) {
        console.error('❌ DELETE /favorites error:', err);
        return res.status(500).json({ message: 'Failed to remove favorite: ' + err.message });
    }
});

// GET /favorites/ids — car IDs the current user has favorited
app.get('/favorites/ids', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    try {
        const result = await pool.query(
            'SELECT car_id FROM favorites WHERE user_id = $1',
            [userId]
        );
        const carIds = result.rows.map(r => r.car_id);
        console.log(`✅ Favorite IDs: ${carIds.length} for user ${userId}`);
        return res.status(200).json({ car_ids: carIds });
    } catch (err) {
        console.error('❌ GET /favorites/ids error:', err);
        return res.status(500).json({ message: 'Failed to fetch favorite IDs: ' + err.message });
    }
});

// GET /favorites/cars — full car objects for the user's favorites
app.get('/favorites/cars', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;

    try {
        const [carsResult, countResult] = await Promise.all([
            pool.query(
                `SELECT c.*, true AS is_favorited
                 FROM cars c
                 INNER JOIN favorites f ON f.car_id = c.id AND f.user_id = $1
                 ORDER BY f.saved_at DESC
                 LIMIT $2 OFFSET $3`,
                [userId, limit, offset]
            ),
            pool.query(
                'SELECT COUNT(*) FROM favorites WHERE user_id = $1',
                [userId]
            ),
        ]);

        console.log(`✅ Favorite Cars: ${carsResult.rowCount} (page ${page}) for user ${userId}`);
        return res.status(200).json({
            cars: carsResult.rows.map(formatCarResponse),
            total: parseInt(countResult.rows[0].count),
            page,
            limit,
        });
    } catch (err) {
        console.error('❌ GET /favorites/cars error:', err);
        return res.status(500).json({ message: 'Failed to fetch favorite cars: ' + err.message });
    }
});

// GET /favorites — all favorites metadata
app.get('/favorites', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    try {
        const result = await pool.query(
            'SELECT * FROM favorites WHERE user_id = $1 ORDER BY saved_at DESC',
            [userId]
        );
        console.log(`✅ Favorites List: ${result.rowCount} for user ${userId}`);
        return res.status(200).json({ favorites: result.rows });
    } catch (err) {
        console.error('❌ GET /favorites error:', err);
        return res.status(500).json({ message: 'Failed to fetch favorites: ' + err.message });
    }
});

// POST /favorites/sync — merge guest favorites after login
app.post('/favorites/sync', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const { car_ids } = req.body;
    if (!car_ids || !Array.isArray(car_ids)) {
        return res.status(400).json({ message: 'car_ids array is required' });
    }

    try {
        const synced = [];

        for (const carId of car_ids) {
            const favId = 'fav_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);

            // Try inserting; ignore if already exists
            const ins = await pool.query(
                `INSERT INTO favorites (id, user_id, car_id, saved_at, is_synced, source)
                 VALUES ($1, $2, $3, NOW(), true, 'synced')
                 ON CONFLICT (user_id, car_id) DO NOTHING
                 RETURNING *`,
                [favId, userId, carId]
            );

            if (ins.rowCount > 0) {
                synced.push(ins.rows[0]);
            } else {
                // Already exists — return the existing row
                const existing = await pool.query(
                    'SELECT * FROM favorites WHERE user_id = $1 AND car_id = $2 LIMIT 1',
                    [userId, carId]
                );
                if (existing.rowCount > 0) synced.push(existing.rows[0]);
            }
        }

        console.log(`✅ Favorites Synced: ${synced.length} for user ${userId}`);
        return res.status(200).json({ favorites: synced });
    } catch (err) {
        console.error('❌ POST /favorites/sync error:', err);
        return res.status(500).json({ message: 'Favorites sync failed: ' + err.message });
    }
});


// ═══════════════════════════════════════════════════════════════════════════
//  CHAT ROUTES
// ═══════════════════════════════════════════════════════════════════════════

// GET /conversations — list the authenticated user's conversations
app.get('/conversations', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    try {
        // JOIN users table to get fresh buyer/seller names and avatars.
        const result = await pool.query(
            `SELECT c.*,
                    COALESCE((c.unread_counts ->> $1)::int, 0) AS unread_count,
                    TRIM(COALESCE(b.user_firstname, '') || ' ' || COALESCE(b.user_lastname, '')) AS buyer_name,
                    b.user_avatar_url AS buyer_avatar_url,
                    TRIM(COALESCE(s.user_firstname, '') || ' ' || COALESCE(s.user_lastname, '')) AS seller_name,
                    s.user_avatar_url AS seller_avatar_url,
                    lm.sender_id AS last_message_sender_id,
                    (lm.status = 'read') AS last_message_is_read
             FROM  conversations c
             LEFT JOIN users b ON c.buyer_id  = b.user_id
             LEFT JOIN users s ON c.seller_id = s.user_id
             LEFT JOIN LATERAL (
                 SELECT sender_id, status
                 FROM messages m
                 WHERE m.conversation_id = c.id
                 ORDER BY m.created_at DESC
                 LIMIT 1
             ) lm ON true
             WHERE c.buyer_id = $1 OR c.seller_id = $1
             ORDER BY c.last_message_at DESC NULLS LAST`,
            [userId]
        );

        console.log(`✅ Conversations List: ${result.rowCount} found for ${userId}`);
        return res.status(200).json(result.rows);
    } catch (err) {
        console.error('❌ GET /conversations error:', err);
        return res.status(500).json({ message: 'Failed to fetch conversations: ' + err.message });
    }
});

// POST /conversations — start (or return an existing) conversation
app.post('/conversations', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const { seller_id, listing_id } = req.body;
    if (!seller_id || !listing_id) {
        return res.status(400).json({ message: 'seller_id and listing_id are required' });
    }
    if (userId === seller_id) {
        return res.status(400).json({ message: 'Cannot start a conversation with yourself' });
    }

    try {
        // Return existing conversation for this buyer/seller/listing triple
        // JOIN users to attach fresh buyer/seller names and avatars.
        const existing = await pool.query(
            `SELECT c.*,
                    COALESCE((c.unread_counts ->> $1)::int, 0) AS unread_count,
                    TRIM(COALESCE(b.user_firstname, '') || ' ' || COALESCE(b.user_lastname, '')) AS buyer_name,
                    b.user_avatar_url AS buyer_avatar_url,
                    TRIM(COALESCE(s.user_firstname, '') || ' ' || COALESCE(s.user_lastname, '')) AS seller_name,
                    s.user_avatar_url AS seller_avatar_url,
                    lm.sender_id AS last_message_sender_id,
                    (lm.status = 'read') AS last_message_is_read
             FROM  conversations c
             LEFT JOIN users b ON c.buyer_id  = b.user_id
             LEFT JOIN users s ON c.seller_id = s.user_id
             LEFT JOIN LATERAL (
                 SELECT sender_id, status
                 FROM messages m
                 WHERE m.conversation_id = c.id
                 ORDER BY m.created_at DESC
                 LIMIT 1
             ) lm ON true
             WHERE c.buyer_id = $1
               AND c.seller_id = $2
               AND c.car_reference ->> 'listing_id' = $3
             LIMIT 1`,
            [userId, seller_id, listing_id]
        );
        if (existing.rowCount > 0) {
            console.log(`✅ Existing conversation returned: ${existing.rows[0].id}`);
            return res.status(200).json(existing.rows[0]);
        }

        // Look up buyer, seller, and the car listing
        const [buyerRes, sellerRes, carRes] = await Promise.all([
            pool.query('SELECT user_firstname, user_lastname, user_avatar_url FROM users WHERE user_id = $1 LIMIT 1', [userId]),
            pool.query('SELECT user_firstname, user_lastname, user_avatar_url FROM users WHERE user_id = $1 LIMIT 1', [seller_id]),
            pool.query('SELECT title, price, image_urls, city, location, is_available FROM cars WHERE id = $1 LIMIT 1', [listing_id]),
        ]);

        const buyer = buyerRes.rowCount > 0 ? buyerRes.rows[0] : null;
        const seller = sellerRes.rowCount > 0 ? sellerRes.rows[0] : null;
        const car = carRes.rowCount > 0 ? carRes.rows[0] : null;

        const convId = 'conv_' + Date.now();
        const now = new Date().toISOString();

        const carReference = {
            listing_id,
            title: car ? car.title : 'Unknown Car',
            price: car ? parseFloat(car.price || 0) : 0,
            thumbnail_url: car && car.image_urls && car.image_urls.length > 0 ? car.image_urls[0] : null,
            city: car ? (car.city || car.location || 'Unknown') : 'Unknown',
            is_active: car ? (car.is_available !== false) : true,
        };

        const unreadCounts = { [userId]: 0, [seller_id]: 0 };

        // Insert WITHOUT buyer/seller name/avatar — those columns are removed.
        const result = await pool.query(
            `INSERT INTO conversations
               (id, buyer_id, seller_id,
                car_reference, last_message_preview, last_message_at,
                unread_counts, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             RETURNING *`,
            [
                convId,
                userId,
                seller_id,
                JSON.stringify(carReference),
                null,
                null,
                JSON.stringify(unreadCounts),
                now,
            ]
        );

        // Attach dynamic user info to the response (mirrors the JOIN shape).
        const buyerName = buyer
            ? `${buyer.user_firstname} ${buyer.user_lastname}`.trim()
            : 'Buyer';
        const sellerName = seller
            ? `${seller.user_firstname} ${seller.user_lastname}`.trim()
            : 'Seller';

        const newConversation = {
            ...result.rows[0],
            unread_count: 0,
            buyer_name: buyerName,
            buyer_avatar_url: buyer ? buyer.user_avatar_url : null,
            seller_name: sellerName,
            seller_avatar_url: seller ? seller.user_avatar_url : null,
            last_message_sender_id: null,
            last_message_is_read: false,
        };
        console.log(`✅ Conversation Created: ${convId}`);

        // Notify the seller in real-time
        broadcastToUser(seller_id, 'conversation.updated', newConversation);

        return res.status(201).json(newConversation);
    } catch (err) {
        console.error('❌ POST /conversations error:', err);
        return res.status(500).json({ message: 'Failed to create conversation: ' + err.message });
    }
});

// GET /conversations/:id/messages — paginated message list
app.get('/conversations/:id/messages', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const conversationId = req.params.id;
    const pageSize = parseInt(req.query.page_size) || 30;
    const before = req.query.before; // message ID cursor

    try {
        let messages;

        if (before) {
            // Cursor-based pagination: return `pageSize` messages before this ID
            const cursorRes = await pool.query(
                'SELECT created_at FROM messages WHERE id = $1 LIMIT 1',
                [before]
            );
            if (cursorRes.rowCount === 0) {
                return res.status(200).json([]);
            }
            const cursorTime = cursorRes.rows[0].created_at;

            const result = await pool.query(
                `SELECT * FROM (
                    SELECT * FROM messages
                    WHERE conversation_id = $1 AND created_at < $2
                    ORDER BY created_at DESC
                    LIMIT $3
                 ) sub
                 ORDER BY created_at ASC`,
                [conversationId, cursorTime, pageSize]
            );
            messages = result.rows;
        } else {
            // Most recent page
            const result = await pool.query(
                `SELECT * FROM (
                    SELECT * FROM messages
                    WHERE conversation_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                 ) sub
                 ORDER BY created_at ASC`,
                [conversationId, pageSize]
            );
            messages = result.rows;
        }

        console.log(`✅ Messages for ${conversationId}: ${messages.length} returned`);
        return res.status(200).json(messages);
    } catch (err) {
        console.error('❌ GET /conversations/:id/messages error:', err);
        return res.status(500).json({ message: 'Failed to fetch messages: ' + err.message });
    }
});

// POST /conversations/:id/messages — send a message
app.post('/conversations/:id/messages', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const conversationId = req.params.id;
    const { text, local_id } = req.body;

    if (!text || !local_id) {
        return res.status(400).json({ message: 'text and local_id are required' });
    }

    try {
        // Verify the conversation exists
        const convRes = await pool.query(
            'SELECT * FROM conversations WHERE id = $1 LIMIT 1',
            [conversationId]
        );
        if (convRes.rowCount === 0) {
            return res.status(404).json({ message: 'Conversation not found' });
        }
        const conversation = convRes.rows[0];

        // Persist the message
        const msgId = 'msg_' + Date.now();
        const now = new Date().toISOString();

        const msgResult = await pool.query(
            `INSERT INTO messages
               (id, local_id, conversation_id, sender_id, text,
                status, created_at, updated_at, retry_count)
             VALUES ($1,$2,$3,$4,$5,'sent',$6,$7,0)
             RETURNING *`,
            [msgId, local_id, conversationId, userId, text, now, now]
        );
        const newMessage = msgResult.rows[0];

        // Update conversation preview
        const preview = text.length > 80 ? text.substring(0, 77) + '…' : text;
        await pool.query(
            `UPDATE conversations
             SET last_message_preview = $1, last_message_at = $2
             WHERE id = $3`,
            [preview, now, conversationId]
        );

        console.log(`✅ Message Sent in ${conversationId} by ${userId}`);

        // Determine the other participant
        const recipientId = conversation.buyer_id === userId
            ? conversation.seller_id
            : conversation.buyer_id;

        // Broadcast message.new to recipient
        broadcastToUser(recipientId, 'message.new', newMessage);

        // Atomically increment recipient's unread counter in JSONB
        await pool.query(
            `UPDATE conversations
             SET unread_counts = jsonb_set(
                 COALESCE(unread_counts, '{}'::jsonb),
                 ARRAY[$1::text],
                 to_jsonb(COALESCE((unread_counts ->> $1)::int, 0) + 1)
             )
             WHERE id = $2`,
            [recipientId, conversationId]
        );

        // Re-fetch the conversation with JOINed user info for the broadcast.
        const updatedConvRes = await pool.query(
            `SELECT c.*,
                    COALESCE((c.unread_counts ->> $1)::int, 0) AS unread_count,
                    TRIM(COALESCE(b.user_firstname, '') || ' ' || COALESCE(b.user_lastname, '')) AS buyer_name,
                    b.user_avatar_url AS buyer_avatar_url,
                    TRIM(COALESCE(s.user_firstname, '') || ' ' || COALESCE(s.user_lastname, '')) AS seller_name,
                    s.user_avatar_url AS seller_avatar_url,
                    lm.sender_id AS last_message_sender_id,
                    (lm.status = 'read') AS last_message_is_read
             FROM  conversations c
             LEFT JOIN users b ON c.buyer_id  = b.user_id
             LEFT JOIN users s ON c.seller_id = s.user_id
             LEFT JOIN LATERAL (
                 SELECT sender_id, status
                 FROM messages m
                 WHERE m.conversation_id = c.id
                 ORDER BY m.created_at DESC
                 LIMIT 1
             ) lm ON true
             WHERE c.id = $2`,
            [recipientId, conversationId]
        );

        if (updatedConvRes.rowCount > 0) {
            broadcastToUser(recipientId, 'conversation.updated', updatedConvRes.rows[0]);
        }

        return res.status(201).json(newMessage);
    } catch (err) {
        console.error('❌ POST /conversations/:id/messages error:', err);
        return res.status(500).json({ message: 'Failed to send message: ' + err.message });
    }
});

// POST /conversations/:id/read — mark messages in a conversation as read
app.post('/conversations/:id/read', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const conversationId = req.params.id;

    try {
        const convRes = await pool.query(
            'SELECT id FROM conversations WHERE id = $1 LIMIT 1',
            [conversationId]
        );
        if (convRes.rowCount === 0) {
            return res.status(404).json({ message: 'Conversation not found' });
        }

        // Mark all messages NOT from the current user as 'read' and get back the updated rows
        const updatedMsgs = await pool.query(
            `UPDATE messages
             SET    status     = 'read',
                    updated_at = NOW()
             WHERE  conversation_id = $1
               AND  sender_id      != $2
               AND  status         != 'read'
             RETURNING local_id, sender_id`,
            [conversationId, userId]
        );

        // Notify each original sender that their messages were read
        const notifiedSenders = new Set();
        for (const msg of updatedMsgs.rows) {
            notifiedSenders.add(msg.sender_id);
        }

        // Reset only the current user's unread counter to 0
        await pool.query(
            `UPDATE conversations
             SET unread_counts = jsonb_set(
                 COALESCE(unread_counts, '{}'::jsonb),
                 ARRAY[$1::text],
                 '0'::jsonb
             )
             WHERE id = $2`,
            [userId, conversationId]
        );

        // Broadcast a fresh conversation snapshot to each sender so their
        // conversation list updates the read indicator in real-time.
        if (notifiedSenders.size > 0) {
            for (const senderId of notifiedSenders) {
                const freshConv = await pool.query(
                    `SELECT c.*,
                            COALESCE((c.unread_counts ->> $1)::int, 0) AS unread_count,
                            TRIM(COALESCE(b.user_firstname, '') || ' ' || COALESCE(b.user_lastname, '')) AS buyer_name,
                            b.user_avatar_url AS buyer_avatar_url,
                            TRIM(COALESCE(s.user_firstname, '') || ' ' || COALESCE(s.user_lastname, '')) AS seller_name,
                            s.user_avatar_url AS seller_avatar_url,
                            lm.sender_id AS last_message_sender_id,
                            (lm.status = 'read') AS last_message_is_read
                     FROM  conversations c
                     LEFT JOIN users b ON c.buyer_id  = b.user_id
                     LEFT JOIN users s ON c.seller_id = s.user_id
                     LEFT JOIN LATERAL (
                         SELECT sender_id, status
                         FROM messages m
                         WHERE m.conversation_id = c.id
                         ORDER BY m.created_at DESC
                         LIMIT 1
                     ) lm ON true
                     WHERE c.id = $2`,
                    [senderId, conversationId]
                );
                if (freshConv.rowCount > 0) {
                    broadcastToUser(senderId, 'conversation.updated', freshConv.rows[0]);
                }
            }
        }

        console.log(`✅ Messages marked as read in ${conversationId} by ${userId}`);
        return res.status(200).json({ message: 'Messages marked as read' });
    } catch (err) {
        console.error('❌ POST /conversations/:id/read error:', err);
        return res.status(500).json({ message: 'Failed to mark messages as read: ' + err.message });
    }
});

// GET /messages/missed — messages updated since a given timestamp
app.get('/messages/missed', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const since = req.query.since;
    if (!since) {
        return res.status(400).json({ message: 'since query parameter is required' });
    }

    const sinceDate = new Date(since);
    if (isNaN(sinceDate.getTime())) {
        return res.status(400).json({ message: 'Invalid since timestamp' });
    }

    try {
        const result = await pool.query(
            `SELECT m.*
             FROM   messages m
             INNER JOIN conversations c ON c.id = m.conversation_id
             WHERE  (c.buyer_id = $1 OR c.seller_id = $1)
               AND  GREATEST(m.updated_at, m.created_at) > $2
             ORDER BY m.created_at ASC`,
            [userId, sinceDate.toISOString()]
        );

        console.log(`✅ Missed Messages: ${result.rowCount} since ${since}`);
        return res.status(200).json(result.rows);
    } catch (err) {
        console.error('❌ GET /messages/missed error:', err);
        return res.status(500).json({ message: 'Failed to fetch missed messages: ' + err.message });
    }
});


// ═══════════════════════════════════════════════════════════════════════════
//  WEBSOCKET SERVER
// ═══════════════════════════════════════════════════════════════════════════

const server = http.createServer(app);

// Map of userId → Set<WebSocket> (one user can have multiple connections)
const connectedClients = new Map();

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
    // Extract token from query string: ws://host:port?token=xxx
    const queryParams = url.parse(req.url, true).query;
    const token = queryParams.token;

    let userId = null;
    if (token) {
        try {
            const payload = jwt.verify(token, JWT_SECRET);
            userId = payload.sub;
        } catch (e) {
            console.log('[ws] ⚠️  Invalid or expired token, closing connection.');
            ws.close(4001, 'Invalid token');
            return;
        }
    }

    if (!userId) {
        ws.close(4001, 'Missing token');
        return;
    }

    if (!connectedClients.has(userId)) {
        connectedClients.set(userId, new Set());
    }
    connectedClients.get(userId).add(ws);
    console.log(`[ws] ✅ User connected: ${userId} (${connectedClients.get(userId).size} sockets)`);

    ws.on('close', () => {
        const sockets = connectedClients.get(userId);
        if (sockets) {
            sockets.delete(ws);
            if (sockets.size === 0) connectedClients.delete(userId);
        }
        console.log(`[ws] 🔌 User disconnected: ${userId}`);
    });

    ws.on('error', (err) => {
        console.error(`[ws] ❌ Error for ${userId}:`, err.message);
    });

    ws.on('message', (raw) => {
        try {
            const data = JSON.parse(raw.toString());
            console.log(`[ws] 📨 Message from ${userId}:`, data);
            // Future: handle typing indicators, etc.
        } catch (e) {
            console.log(`[ws] ⚠️  Malformed message from ${userId}`);
        }
    });
});

/**
 * Sends a typed event to all active WebSocket connections of a given user.
 * @param {string} userId  - target user
 * @param {string} type    - event type ('message.new', 'message.status', 'conversation.updated')
 * @param {object} payload - event data
 */
function broadcastToUser(userId, type, payload) {
    const sockets = connectedClients.get(userId);
    if (!sockets || sockets.size === 0) {
        console.log(`[ws] 📭 No active socket for ${userId}, event queued for REST sync.`);
        return;
    }
    const msg = JSON.stringify({ type, payload });
    for (const ws of sockets) {
        if (ws.readyState === 1) { // WebSocket.OPEN
            ws.send(msg);
            console.log(`[ws] 📤 Sent ${type} to ${userId}`);
        }
    }
}


// ─── Root & Error Handling ────────────────────────────────────────────────────

app.get('/', (req, res) => {
    return res.status(200).json({ message: 'TurboCar Server Running', wsEnabled: true, db: 'PostgreSQL' });
});

app.use((req, res) => res.status(404).json({ message: 'Route not found' }));

app.use((err, req, res, next) => {
    console.error('SERVER ERROR:', err);
    res.status(500).json({ message: 'Internal Server Error' });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 TurboCar Server running at port ${PORT}`);
    console.log(`🔌 WebSocket server active on ws://localhost:${PORT}`);
    console.log(`🗄️  Database: PostgreSQL (Supabase)`);
});