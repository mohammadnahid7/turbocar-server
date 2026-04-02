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
const bcrypt = require('bcryptjs');
const { S3Client, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { Pool } = require('pg');
const admin = require('firebase-admin');

// ─── Firebase Admin SDK Initialization ───────────────────────────────────────
let firebaseAdminInitialized = false;
try {
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        admin.initializeApp({
            credential: admin.credential.cert(serviceAccount)
        });
        firebaseAdminInitialized = true;
        console.log('✅ Firebase Admin SDK initialized successfully (via .env)');
    } else {
        console.warn('⚠️ FIREBASE_SERVICE_ACCOUNT not found in .env! Push notifications will be disabled.');
    }
} catch (err) {
    console.error('❌ Failed to initialize Firebase Admin SDK:', err.message);
}

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
const BCRYPT_SALT_ROUNDS = 12;

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

/**
 * Delete a file from Cloudflare R2 by its public URL.
 * Silently fails — cleanup is best-effort, never blocks the caller.
 *
 * @param {string} fileUrl - Full public URL returned by uploadToR2
 */
async function deleteFromR2(fileUrl) {
    if (!r2Enabled || !s3Client || !fileUrl) return;
    try {
        // Extract the key from the public URL (everything after the R2_PUBLIC_URL prefix)
        const key = fileUrl.replace(`${R2_PUBLIC_URL}/`, '');
        if (!key || key === fileUrl) return; // Not an R2 URL

        await s3Client.send(new DeleteObjectCommand({
            Bucket: R2_BUCKET_NAME,
            Key: key,
        }));
        console.log(`  🗑️  Deleted from R2: ${key}`);
    } catch (err) {
        console.warn(`  ⚠️  R2 cleanup failed for ${fileUrl}: ${err.message}`);
    }
}

// ─── Helper: generate real JWT tokens ───────────────────────────────────────
function generateTokens(userId, tokenVersion = 0) {
    const access_token = jwt.sign({ sub: userId, type: 'access' }, JWT_SECRET, {
        expiresIn: JWT_ACCESS_EXPIRY,
    });
    const refresh_token = jwt.sign({ sub: userId, type: 'refresh', v: tokenVersion }, JWT_SECRET, {
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
        seller_avatar: car.seller_avatar || null,
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

        // ── Password verification (bcrypt) ──────────────────────────────
        if (!user.password_hash) {
            return res.status(401).json({ message: 'Account requires password reset. Please re-register.' });
        }
        const passwordValid = await bcrypt.compare(user_password, user.password_hash);
        if (!passwordValid) {
            return res.status(401).json({ message: 'Invalid email or password' });
        }

        console.log(`✅ Login Success for: ${user_email}`);
        const tokens = generateTokens(user.user_id, user.token_version || 0);
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
            user_phone, user_password,
            user_dob, user_gender,
        } = req.body;
        const finalEmail = user_email || email;

        // ── Validate password is present and meets minimum length ────────
        if (!user_password || user_password.length < 8) {
            return res.status(400).json({ message: 'Password is required and must be at least 8 characters' });
        }

        // Return existing user if already registered (idempotent)
        const existing = await pool.query(
            'SELECT * FROM users WHERE user_email = $1 LIMIT 1',
            [finalEmail]
        );
        if (existing.rowCount > 0) {
            return res.status(409).json({ message: 'An account with this email already exists. Please login.' });
        }

        let avatarUrl = null;
        if (req.file) {
            avatarUrl = await uploadToR2(req.file, 'users/');
            console.log(`  📸 Avatar uploaded: ${req.file.originalname}`);
        }

        // ── Hash password with bcrypt ────────────────────────────────────
        const passwordHash = await bcrypt.hash(user_password, BCRYPT_SALT_ROUNDS);

        const userId = 'user_' + Date.now();
        const now = new Date().toISOString();

        const insert = await pool.query(
            `INSERT INTO users
               (user_id, user_firstname, user_lastname, user_phone, user_email,
                user_avatar_url, user_dob, user_gender, user_role,
                user_is_verified, password_hash, token_version,
                user_created_at, user_updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
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
                'buyer',            // ← Hardcoded role — prevents privilege escalation
                true,
                passwordHash,
                0,                  // token_version starts at 0
                now,
                now,
            ]
        );

        const newUser = insert.rows[0];
        console.log(`✅ Register Success for: ${finalEmail}`);
        const tokens = generateTokens(newUser.user_id, 0);
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

        // ── Token version check — revokes old refresh tokens after logout ─
        const currentVersion = user.token_version || 0;
        const tokenVersion = payload.v !== undefined ? payload.v : 0;
        if (tokenVersion !== currentVersion) {
            return res.status(401).json({ message: 'Refresh token has been revoked' });
        }

        const tokens = generateTokens(user.user_id, currentVersion);
        console.log(`✅ Token Refreshed for: ${user.user_email}`);
        return res.status(200).json({ ...tokens, user: buildUserResponse(user) });
    } catch (e) {
        return res.status(401).json({ message: 'Invalid or expired refresh token' });
    }
});

// POST /auth/logout — revoke all refresh tokens for the authenticated user
app.post('/auth/logout', authenticateToken, async (req, res) => {
    try {
        await pool.query(
            'UPDATE users SET token_version = COALESCE(token_version, 0) + 1 WHERE user_id = $1',
            [req.userId]
        );
        console.log(`✅ Logout (tokens revoked) for: ${req.userId}`);
        return res.status(200).json({ message: 'Logged out successfully' });
    } catch (err) {
        console.error('❌ Logout error:', err);
        return res.status(500).json({ message: 'Logout failed: ' + err.message });
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
            const user = result.rows[0];
            const tokens = generateTokens(user.user_id, user.token_version || 0);
            return res.status(200).json({
                exists: true,
                ...tokens,
                user: buildUserResponse(user),
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
            user_phone, user_dob, user_gender,
        } = req.body;
        // NOTE: `role` deliberately NOT destructured — prevents privilege escalation

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
                user_is_verified, token_version, user_created_at, user_updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
                'buyer',            // ← Hardcoded role — prevents privilege escalation
                avatarUrl,
                true,
                0,                  // token_version
                now,
                now,
            ]
        );

        const newUser = result.rows[0];
        console.log(`✅ Google Register Success for: ${finalEmail}`);
        const tokens = generateTokens(newUser.user_id, newUser.token_version || 0);
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
           u.user_avatar_url AS seller_avatar,
           CASE WHEN f.car_id IS NOT NULL THEN true ELSE false END AS is_favorited
    FROM   cars c
    LEFT JOIN users u ON u.user_id = c.seller_id
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

app.post('/cars', authenticateToken, upload.array('images', 10), async (req, res) => {
    console.log('✅ Car API Hit (POST Create)');
    try {
        // seller_id is now securely extracted via authenticateToken middleware
        const sellerId = req.userId;

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

app.get('/cars/suggestions', async (req, res) => {
    const query = (req.query.q || '').trim().toLowerCase();
    const limit = Math.min(parseInt(req.query.limit) || 10, 20);

    // Edge case: empty or missing query — return immediately
    if (!query) {
        return res.status(200).json({ suggestions: [] });
    }

    try {
        const likeQuery = `${query}%`; // Prefix only — no leading wildcard

        const result = await pool.query(
            `SELECT DISTINCT text, type FROM (
                SELECT DISTINCT brand AS text, 'brand' AS type
                FROM cars
                WHERE lower(brand) LIKE $1
                UNION
                SELECT DISTINCT model AS text, 'model' AS type
                FROM cars
                WHERE lower(model) LIKE $1
            ) AS combined
            ORDER BY text
            LIMIT $2`,
            [likeQuery, limit]
        );

        return res.status(200).json({
            suggestions: result.rows.map(row => ({
                type: row.type,
                text: row.text,
            })),
        });
    } catch (err) {
        console.error('❌ GET /cars/suggestions error:', err);
        return res.status(500).json({ message: 'Suggestions failed: ' + err.message });
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

        // Notify the car owner
        try {
            const carQuery = await pool.query('SELECT seller_id, title FROM cars WHERE id = $1', [car_id]);
            if (carQuery.rowCount > 0) {
                const car = carQuery.rows[0];
                if (car.seller_id && car.seller_id !== userId) {
                    const userQuery = await pool.query('SELECT user_firstname, user_lastname FROM users WHERE user_id = $1', [userId]);
                    const userName = userQuery.rowCount > 0 && userQuery.rows[0].user_firstname ? userQuery.rows[0].user_firstname + ' ' + userQuery.rows[0].user_lastname : 'Someone';

                    await sendNotification({
                        userId: car.seller_id,
                        type: 'CAR_FAVORITED',
                        title: 'New Favorite! ❤️',
                        body: `${userName} favorited your ${car.title}`,
                        data: { carId: car_id, route: `/car/${car_id}` }
                    });
                }
            }
        } catch (notifErr) {
            console.error('❌ Favorite notification error:', notifErr.message);
        }

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


// ═══════════════════════════════════════════════════════════════════════════
//  NOTIFICATIONS
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Central notification dispatcher.
 * Saves to DB → broadcasts via WebSocket → (future) sends FCM push.
 */
async function sendNotification({ userId, type, title, body, data = {} }) {
    try {
        const result = await pool.query(
            `INSERT INTO notifications (user_id, type, title, body, data)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING *`,
            [userId, type, title, body, JSON.stringify(data)]
        );
        const notification = result.rows[0];

        // 1. Real-time delivery via WebSocket (In-App)
        broadcastToUser(userId, 'notification.new', notification);

        // 2. Push Notification delivery via Firebase (Background)
        if (firebaseAdminInitialized) {
            try {
                // Fetch user device tokens
                const tokensResult = await pool.query(
                    'SELECT token FROM user_device_tokens WHERE user_id = $1',
                    [userId]
                );

                if (tokensResult.rowCount > 0) {
                    const tokens = tokensResult.rows.map(row => row.token);

                    const message = {
                        notification: {
                            title: title,
                            body: body
                        },
                        data: Object.fromEntries(
                            Object.entries(data).map(([key, val]) => [key, String(val)]) // FCM data values must be strings
                        ),
                        tokens: tokens
                    };

                    const response = await admin.messaging().sendEachForMulticast(message);
                    console.log(`📨 FCM pushed to ${response.successCount} devices, ${response.failureCount} failed.`);

                    // Optional: Clean up failed tokens if needed in the future
                }
            } catch (fcmErr) {
                console.error('❌ FCM dispatch error:', fcmErr.message);
            }
        }

        console.log(`🔔 Notification processed for ${userId}: [${type}] ${title}`);
        return notification;
    } catch (err) {
        console.error('❌ sendNotification error:', err.message);
    }
}

// GET /notifications — fetch paginated notifications for the authenticated user
app.get('/notifications', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 30;
    const offset = (page - 1) * limit;

    try {
        const result = await pool.query(
            `SELECT * FROM notifications
             WHERE user_id = $1
             ORDER BY created_at DESC
             LIMIT $2 OFFSET $3`,
            [userId, limit, offset]
        );

        const countResult = await pool.query(
            'SELECT COUNT(*) FROM notifications WHERE user_id = $1',
            [userId]
        );

        const unreadResult = await pool.query(
            'SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = false',
            [userId]
        );

        return res.status(200).json({
            notifications: result.rows,
            total: parseInt(countResult.rows[0].count),
            unread_count: parseInt(unreadResult.rows[0].count),
            page,
            limit,
        });
    } catch (err) {
        console.error('❌ GET /notifications error:', err.message);
        return res.status(500).json({ message: 'Failed to fetch notifications' });
    }
});

// PATCH /notifications/:id/read — mark a single notification as read
app.patch('/notifications/:id/read', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    try {
        const result = await pool.query(
            `UPDATE notifications SET is_read = true
             WHERE id = $1 AND user_id = $2
             RETURNING *`,
            [req.params.id, userId]
        );
        if (result.rowCount === 0) return res.status(404).json({ message: 'Notification not found' });
        return res.status(200).json(result.rows[0]);
    } catch (err) {
        console.error('❌ PATCH /notifications/:id/read error:', err.message);
        return res.status(500).json({ message: 'Failed to mark notification as read' });
    }
});

// PATCH /notifications/read-all — mark all notifications as read
app.patch('/notifications/read-all', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    try {
        await pool.query(
            'UPDATE notifications SET is_read = true WHERE user_id = $1 AND is_read = false',
            [userId]
        );
        return res.status(200).json({ message: 'All notifications marked as read' });
    } catch (err) {
        console.error('❌ PATCH /notifications/read-all error:', err.message);
        return res.status(500).json({ message: 'Failed to mark all as read' });
    }
});

// DELETE /notifications/:id — delete a notification
app.delete('/notifications/:id', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    try {
        const result = await pool.query(
            'DELETE FROM notifications WHERE id = $1 AND user_id = $2',
            [req.params.id, userId]
        );
        if (result.rowCount === 0) return res.status(404).json({ message: 'Notification not found' });
        return res.status(200).json({ message: 'Notification deleted' });
    } catch (err) {
        console.error('❌ DELETE /notifications/:id error:', err.message);
        return res.status(500).json({ message: 'Failed to delete notification' });
    }
});

// POST /device-tokens — register a device token for push notifications
app.post('/device-tokens', async (req, res) => {
    const userId = extractUserId(req);
    if (!userId) return res.status(401).json({ message: 'Unauthorized' });

    const { token, platform } = req.body;
    if (!token) return res.status(400).json({ message: 'Token is required' });

    try {
        await pool.query(
            `INSERT INTO user_device_tokens (token, user_id, platform, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (token) DO UPDATE SET user_id = $2, platform = $3, updated_at = NOW()`,
            [token, userId, platform || 'unknown']
        );
        return res.status(200).json({ message: 'Device token registered' });
    } catch (err) {
        console.error('❌ POST /device-tokens error:', err.message);
        return res.status(500).json({ message: 'Failed to register device token' });
    }
});


// ═══════════════════════════════════════════════════════════════════════════
//  ADS / CAMPAIGN ROUTES (v1)
// ═══════════════════════════════════════════════════════════════════════════

// ─── Campaign-specific Multer with MIME + size validation ────────────────────
const ALLOWED_THUMBNAIL_MIMES = ['image/jpeg', 'image/png', 'image/webp'];
const ALLOWED_MEDIA_MIMES = ['video/mp4', 'video/webm', 'video/quicktime'];
const MAX_THUMBNAIL_SIZE = 500 * 1024;       // 500 KB
const MAX_MEDIA_SIZE = 50 * 1024 * 1024;     // 50 MB

const campaignUpload = multer({
    dest: path.join(os.tmpdir(), 'turbocar-uploads'),
    limits: { fileSize: MAX_MEDIA_SIZE }, // global ceiling; per-field checked after
    fileFilter: (req, file, cb) => {
        if (file.fieldname === 'thumbnail') {
            if (!ALLOWED_THUMBNAIL_MIMES.includes(file.mimetype)) {
                return cb(new Error(`Invalid thumbnail type: ${file.mimetype}. Allowed: ${ALLOWED_THUMBNAIL_MIMES.join(', ')}`));
            }
        } else if (file.fieldname === 'media') {
            if (!ALLOWED_MEDIA_MIMES.includes(file.mimetype)) {
                return cb(new Error(`Invalid media type: ${file.mimetype}. Allowed: ${ALLOWED_MEDIA_MIMES.join(', ')}`));
            }
        }
        cb(null, true);
    },
}).fields([
    { name: 'media', maxCount: 1 },
    { name: 'thumbnail', maxCount: 1 },
]);

/**
 * Wraps campaignUpload to catch multer errors (size/type) and return 400.
 */
function campaignUploadMiddleware(req, res, next) {
    campaignUpload(req, res, (err) => {
        if (err) {
            console.error('❌ Campaign upload validation error:', err.message);
            return res.status(400).json({ message: err.message });
        }
        // Per-field size enforcement (multer limits is global, not per-field)
        const thumbnailFile = req.files && req.files['thumbnail'] && req.files['thumbnail'][0];
        if (thumbnailFile && thumbnailFile.size > MAX_THUMBNAIL_SIZE) {
            // Clean up temp file
            fs.unlink(thumbnailFile.path, () => { });
            return res.status(400).json({ message: `Thumbnail exceeds maximum size of ${MAX_THUMBNAIL_SIZE / 1024}KB` });
        }
        next();
    });
}

// ─── Middleware: require admin role ──────────────────────────────────────────
async function requireAdmin(req, res, next) {
    try {
        const result = await pool.query(
            'SELECT user_role FROM users WHERE user_id = $1 LIMIT 1',
            [req.userId]
        );
        if (result.rowCount === 0) {
            return res.status(404).json({ message: 'User not found' });
        }
        if (result.rows[0].user_role !== 'admin') {
            return res.status(403).json({ message: 'Forbidden: Admin access required' });
        }
        next();
    } catch (err) {
        console.error('❌ requireAdmin error:', err);
        return res.status(500).json({ message: 'Authorization check failed' });
    }
}

// ─── Target Validators (creation-time only) ─────────────────────────────────
// Extensible registry: add new target types here without if/else chains.
const TargetValidators = {
    car: async (id) => {
        const res = await pool.query(
            'SELECT is_available FROM cars WHERE id = $1 LIMIT 1',
            [id]
        );
        return res.rowCount > 0 && res.rows[0].is_available === true;
    },
    // Future: seller, brand, collection, etc.
};

// ─── Helper: format campaign for public API (camelCase) ─────────────────────
function formatCampaignResponse(row) {
    return {
        campaignId: row.id,
        targetType: row.target_type,
        targetId: row.target_id,
        mediaUrl: row.media_url,
        mediaType: row.media_type,
        thumbnailUrl: row.thumbnail_url || null,
        priority: row.priority != null ? parseInt(row.priority, 10) : 0,
    };
}

// ─── Admin: Create Campaign ─────────────────────────────────────────────────
app.post('/admin/campaigns', authenticateToken, requireAdmin, campaignUploadMiddleware, async (req, res) => {
    console.log('✅ Campaign API Hit (POST Create)');
    try {
        const {
            target_type, target_id, media_type,
            start_date, end_date, priority, source_type, status,
        } = req.body;

        // Validate required fields
        if (!target_type || !target_id || !start_date || !end_date) {
            return res.status(400).json({ message: 'target_type, target_id, start_date, and end_date are required' });
        }

        // Validate dates
        const startParsed = new Date(start_date);
        const endParsed = new Date(end_date);
        if (isNaN(startParsed.getTime()) || isNaN(endParsed.getTime())) {
            return res.status(400).json({ message: 'Invalid date format for start_date or end_date' });
        }
        if (endParsed <= startParsed) {
            return res.status(400).json({ message: 'end_date must be after start_date' });
        }

        // Validate target exists using registry
        const validator = TargetValidators[target_type];
        if (!validator) {
            return res.status(400).json({ message: `Unsupported target_type: ${target_type}` });
        }
        const targetValid = await validator(target_id);
        if (!targetValid) {
            return res.status(400).json({ message: `Target ${target_type} with id ${target_id} not found or unavailable` });
        }

        // ── Resolve media URL (file upload or body URL) ─────────────────
        const mediaFile = req.files && req.files['media'] && req.files['media'][0];
        let mediaUrl = req.body.media_url || null;
        if (mediaFile) {
            mediaUrl = await uploadToR2(mediaFile, 'campaigns/');
            console.log(`  📸 Campaign media uploaded: ${mediaFile.originalname}`);
        }
        if (!mediaUrl) {
            return res.status(400).json({ message: 'media file or media_url is required' });
        }

        // ── Resolve thumbnail URL (file upload or body URL) — REQUIRED ──-
        const thumbnailFile = req.files && req.files['thumbnail'] && req.files['thumbnail'][0];
        let thumbnailUrl = req.body.thumbnail_url || null;
        if (thumbnailFile) {
            try {
                thumbnailUrl = await uploadToR2(thumbnailFile, 'campaigns/thumbnails/');
                console.log(`  🖼️ Campaign thumbnail uploaded: ${thumbnailFile.originalname}`);
            } catch (uploadErr) {
                // Rollback: clean up the media file we already uploaded
                await deleteFromR2(mediaUrl);
                throw uploadErr;
            }
        }
        if (!thumbnailUrl) {
            // Rollback: clean up media if we uploaded it
            if (mediaFile) await deleteFromR2(mediaUrl);
            return res.status(400).json({ message: 'thumbnail file or thumbnail_url is required' });
        }

        // Validate status
        const allowedStatuses = ['draft', 'published', 'paused'];
        const finalStatus = allowedStatuses.includes(status) ? status : 'draft';

        const campId = 'camp_' + Date.now();
        const now = new Date().toISOString();

        let result;
        try {
            result = await pool.query(
                `INSERT INTO ad_campaigns
                   (id, target_type, target_id, media_url, media_type,
                    thumbnail_url, start_date, end_date, priority, status,
                    source_type, created_at, updated_at)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                 RETURNING *`,
                [
                    campId,
                    target_type,
                    target_id,
                    mediaUrl,
                    media_type || 'video',
                    thumbnailUrl,
                    startParsed.toISOString(),
                    endParsed.toISOString(),
                    parseInt(priority) || 0,
                    finalStatus,
                    source_type || 'official',
                    now,
                    now,
                ]
            );
        } catch (dbErr) {
            // Atomic rollback: clean up uploaded files on DB failure
            console.error('❌ DB insert failed, rolling back uploads...');
            await deleteFromR2(mediaUrl);
            await deleteFromR2(thumbnailUrl);
            throw dbErr;
        }

        console.log(`✅ Campaign Created: ${campId}`);
        return res.status(201).json({ campaign: result.rows[0] });
    } catch (err) {
        console.error('❌ POST /admin/campaigns error:', err);
        return res.status(500).json({ message: 'Campaign creation failed: ' + err.message });
    }
});

// ─── Admin: Update Campaign ─────────────────────────────────────────────────
const CAMPAIGN_UPDATABLE_FIELDS = [
    'target_type', 'target_id', 'media_url', 'media_type',
    'thumbnail_url', 'start_date', 'end_date', 'priority', 'status', 'source_type',
];

app.put('/admin/campaigns/:id', authenticateToken, requireAdmin, campaignUploadMiddleware, async (req, res) => {
    console.log(`✅ Campaign API Hit (PUT Update) id=${req.params.id}`);
    try {
        const campId = req.params.id;

        const existing = await pool.query('SELECT * FROM ad_campaigns WHERE id = $1 LIMIT 1', [campId]);
        if (existing.rowCount === 0) {
            return res.status(404).json({ message: 'Campaign not found' });
        }
        const oldCampaign = existing.rows[0];

        const updates = { ...req.body };

        // Track old URLs for cleanup after successful update
        let oldMediaUrl = null;
        let oldThumbnailUrl = null;

        // Handle new media upload
        const mediaFile = req.files && req.files['media'] && req.files['media'][0];
        if (mediaFile) {
            oldMediaUrl = oldCampaign.media_url;
            updates.media_url = await uploadToR2(mediaFile, 'campaigns/');
            console.log(`  📸 Campaign media updated: ${mediaFile.originalname}`);
        }

        // Handle new thumbnail upload (if not provided, keep existing)
        const thumbnailFile = req.files && req.files['thumbnail'] && req.files['thumbnail'][0];
        if (thumbnailFile) {
            try {
                oldThumbnailUrl = oldCampaign.thumbnail_url;
                updates.thumbnail_url = await uploadToR2(thumbnailFile, 'campaigns/thumbnails/');
                console.log(`  🖼️ Campaign thumbnail updated: ${thumbnailFile.originalname}`);
            } catch (uploadErr) {
                // Rollback: clean up new media if we uploaded one
                if (mediaFile && updates.media_url) await deleteFromR2(updates.media_url);
                throw uploadErr;
            }
        } else if (updates.thumbnail_url) {
            // Allow URL-based thumbnail update from body
            oldThumbnailUrl = oldCampaign.thumbnail_url;
        }

        // Validate dates if provided
        if (updates.start_date) {
            const sd = new Date(updates.start_date);
            if (isNaN(sd.getTime())) return res.status(400).json({ message: 'Invalid start_date' });
            updates.start_date = sd.toISOString();
        }
        if (updates.end_date) {
            const ed = new Date(updates.end_date);
            if (isNaN(ed.getTime())) return res.status(400).json({ message: 'Invalid end_date' });
            updates.end_date = ed.toISOString();
        }

        // Validate status if provided
        if (updates.status) {
            const allowedStatuses = ['draft', 'published', 'paused'];
            if (!allowedStatuses.includes(updates.status)) {
                return res.status(400).json({ message: `Invalid status: ${updates.status}. Allowed: ${allowedStatuses.join(', ')}` });
            }
        }

        // Validate target if changed
        if (updates.target_type || updates.target_id) {
            const tType = updates.target_type || oldCampaign.target_type;
            const tId = updates.target_id || oldCampaign.target_id;
            const validator = TargetValidators[tType];
            if (!validator) {
                return res.status(400).json({ message: `Unsupported target_type: ${tType}` });
            }
            const valid = await validator(tId);
            if (!valid) {
                return res.status(400).json({ message: `Target ${tType} with id ${tId} not found or unavailable` });
            }
        }

        // Parse priority to int if provided
        if (updates.priority !== undefined) {
            updates.priority = parseInt(updates.priority) || 0;
        }

        const { text: setClauses, values, nextIndex } = buildSetClause(updates, CAMPAIGN_UPDATABLE_FIELDS, 1);
        if (!setClauses) {
            return res.status(400).json({ message: 'No valid fields to update' });
        }

        const query = `
            UPDATE ad_campaigns
            SET ${setClauses}, updated_at = $${nextIndex}
            WHERE id = $${nextIndex + 1}
            RETURNING *
        `;
        values.push(new Date().toISOString(), campId);

        const result = await pool.query(query, values);
        if (result.rowCount === 0) {
            return res.status(500).json({ message: 'Failed to update campaign' });
        }

        // Async cleanup: delete old files that were replaced
        if (oldMediaUrl) deleteFromR2(oldMediaUrl).catch(() => { });
        if (oldThumbnailUrl) deleteFromR2(oldThumbnailUrl).catch(() => { });

        console.log(`✅ Campaign Updated: ${campId}`);
        return res.status(200).json({ campaign: result.rows[0] });
    } catch (err) {
        console.error('❌ PUT /admin/campaigns/:id error:', err);
        return res.status(500).json({ message: 'Campaign update failed: ' + err.message });
    }
});

// ─── Admin: Delete Campaign ─────────────────────────────────────────────────
app.delete('/admin/campaigns/:id', authenticateToken, requireAdmin, async (req, res) => {
    console.log(`✅ Campaign API Hit (DELETE) id=${req.params.id}`);
    try {
        const campId = req.params.id;

        // Fetch campaign before deleting to get file URLs for cleanup
        const existing = await pool.query(
            'SELECT media_url, thumbnail_url FROM ad_campaigns WHERE id = $1 LIMIT 1',
            [campId]
        );
        if (existing.rowCount === 0) {
            return res.status(404).json({ message: 'Campaign not found' });
        }
        const { media_url, thumbnail_url } = existing.rows[0];

        const result = await pool.query('DELETE FROM ad_campaigns WHERE id = $1 RETURNING id', [campId]);
        if (result.rowCount === 0) {
            return res.status(404).json({ message: 'Campaign not found' });
        }

        // Async cleanup: delete associated files from R2
        if (media_url) deleteFromR2(media_url).catch(() => { });
        if (thumbnail_url) deleteFromR2(thumbnail_url).catch(() => { });

        console.log(`✅ Campaign Deleted: ${campId}`);
        return res.status(200).json({ message: 'Campaign deleted successfully' });
    } catch (err) {
        console.error('❌ DELETE /admin/campaigns/:id error:', err);
        return res.status(500).json({ message: 'Campaign deletion failed: ' + err.message });
    }
});

// ─── Admin: List All Campaigns ──────────────────────────────────────────────
app.get('/admin/campaigns', authenticateToken, requireAdmin, async (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;

    try {
        const [campsResult, countResult] = await Promise.all([
            pool.query(
                `SELECT * FROM ad_campaigns
                 ORDER BY created_at DESC
                 LIMIT $1 OFFSET $2`,
                [limit, offset]
            ),
            pool.query('SELECT COUNT(*) FROM ad_campaigns'),
        ]);

        return res.status(200).json({
            campaigns: campsResult.rows,
            total: parseInt(countResult.rows[0].count),
            page,
            limit,
        });
    } catch (err) {
        console.error('❌ GET /admin/campaigns error:', err);
        return res.status(500).json({ message: 'Failed to fetch campaigns: ' + err.message });
    }
});

// ─── Public: Get Active Campaigns ───────────────────────────────────────────
// Uses SQL JOIN-based validation — zero N+1 queries.
// 'active' is derived: status = 'published' AND NOW() within date window.
app.get('/campaigns', async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const offset = parseInt(req.query.offset) || 0;

    try {
        const [campsResult, countResult] = await Promise.all([
            pool.query(
                `SELECT ad.id, ad.target_type, ad.target_id,
                        ad.media_url, ad.media_type, ad.thumbnail_url, ad.priority
                 FROM   ad_campaigns ad
                 LEFT JOIN cars c ON c.id = ad.target_id AND ad.target_type = 'car'
                 WHERE  ad.status = 'published'
                   AND  NOW() BETWEEN ad.start_date AND ad.end_date
                   AND  ad.thumbnail_url IS NOT NULL
                   AND  (ad.target_type = 'car' AND c.is_available = true)
                 ORDER BY ad.priority DESC, ad.created_at DESC
                 LIMIT $1 OFFSET $2`,
                [limit, offset]
            ),
            pool.query(
                `SELECT COUNT(*) FROM ad_campaigns ad
                 LEFT JOIN cars c ON c.id = ad.target_id AND ad.target_type = 'car'
                 WHERE  ad.status = 'published'
                   AND  NOW() BETWEEN ad.start_date AND ad.end_date
                   AND  ad.thumbnail_url IS NOT NULL
                   AND  (ad.target_type = 'car' AND c.is_available = true)`
            ),
        ]);

        return res.status(200).json({
            campaigns: campsResult.rows.map(formatCampaignResponse),
            total: parseInt(countResult.rows[0].count),
        });
    } catch (err) {
        console.error('❌ GET /campaigns error:', err);
        return res.status(500).json({ message: 'Failed to fetch campaigns: ' + err.message });
    }
});

// ─── Public: Track Campaign Interaction ─────────────────────────────────────
app.post('/campaigns/:id/track', async (req, res) => {
    const campId = req.params.id;
    const { event_type } = req.body;
    const userId = extractUserId(req); // nullable — guests can track too

    if (!event_type || !['click', 'view'].includes(event_type)) {
        return res.status(400).json({ message: 'event_type must be "click" or "view"' });
    }

    try {
        // Validate campaign exists
        const campCheck = await pool.query(
            'SELECT id FROM ad_campaigns WHERE id = $1 LIMIT 1',
            [campId]
        );
        if (campCheck.rowCount === 0) {
            return res.status(404).json({ message: 'Campaign not found' });
        }

        const trackId = 'track_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
        await pool.query(
            `INSERT INTO campaign_tracking (id, campaign_id, user_id, event_type, created_at)
             VALUES ($1, $2, $3, $4, NOW())`,
            [trackId, campId, userId, event_type]
        );

        console.log(`✅ Campaign Tracked: ${event_type} on ${campId}`);
        return res.status(200).json({ success: true });
    } catch (err) {
        console.error('❌ POST /campaigns/:id/track error:', err);
        return res.status(500).json({ message: 'Tracking failed: ' + err.message });
    }
});


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