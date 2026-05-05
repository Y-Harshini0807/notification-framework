const express = require("express")
const axios   = require("axios")
const mongoose = require("mongoose")
const cors = require("cors")
const bcrypt = require("bcrypt")
const jwt = require("jsonwebtoken")
const crypto = require("crypto")
const JWT_SECRET   = "your_secret_key"
const FASTAPI_URL  = process.env.FASTAPI_URL || "http://localhost:8000"
const MONGO_URL    = process.env.MONGO_URL || "mongodb://localhost:27017/notification_db"
const EmployeeModel = require("./models/client")
const TOKEN_REFRESH_INTERVAL_MONTHS = 1

const app = express()
app.use(express.json())
app.use(cors())

mongoose.connect(MONGO_URL)

// -------------------- Utility Functions --------------------
function generateClientId() {
    return crypto.randomBytes(16).toString("hex")
}

function generateApiKey() {
    // nf_ prefix matches the FastAPI verify_api_key format
    return "nf_" + crypto.randomBytes(32).toString("hex")
}

function normalizeEventType(value) {
    return String(value || "").trim().toUpperCase()
}

function addMonths(date, months = TOKEN_REFRESH_INTERVAL_MONTHS) {
    const base = date instanceof Date ? new Date(date) : new Date(date || Date.now())
    const day = base.getDate()
    base.setMonth(base.getMonth() + months)

    if (base.getDate() !== day) {
        base.setDate(0)
    }
    return base
}

function getTokenNextRefreshDate(token) {
    return token.next_refresh_at || token.expires_at || addMonths(token.created_at || Date.now())
}

async function syncPlainApiKeyToFastApi(user, rawKey, eventType, action = "token") {
    try {
        await axios.post(`${FASTAPI_URL}/clients/sync`, {
            client_id:        user.client_id,
            name:             user.client_name || user.name,
            plain_api_key:    rawKey,
            event_type:       eventType,
            allowed_channels: ["email", "sms", "whatsapp", "push"],
            monthly_quota:    user.monthly_quota || 10000000,
        })
        console.log(`[SYNC] ${action} -> FastAPI for ${user.client_id}`)
        return { synced: true }
    } catch (syncErr) {
        console.warn("[SYNC] FastAPI sync failed (non-fatal):", syncErr.message)
        return { synced: false, error: syncErr.message }
    }
}

async function rotateEventToken(user, eventType, reason = "manual") {
    const normalizedEvent = normalizeEventType(eventType)
    const rawKey = generateApiKey()
    const hash = await bcrypt.hash(rawKey, 10)
    const refreshedAt = new Date()
    const nextRefreshAt = addMonths(refreshedAt)

    const result = await EmployeeModel.updateOne(
        {
            email: user.email,
            "event_tokens.event_type": normalizedEvent
        },
        {
            $set: {
                "event_tokens.$.api_key_hash": hash,
                "event_tokens.$.created_at": refreshedAt,
                "event_tokens.$.last_refreshed_at": refreshedAt,
                "event_tokens.$.next_refresh_at": nextRefreshAt,
                "event_tokens.$.expires_at": nextRefreshAt,
                "event_tokens.$.is_active": true,
                "event_tokens.$.auto_refresh_enabled": true
            }
        }
    )

    if (result.matchedCount === 0) {
        return null
    }

    const sync = await syncPlainApiKeyToFastApi(user, rawKey, normalizedEvent, `${reason}-refresh-token`)
    return {
        event_type: normalizedEvent,
        api_key: rawKey,
        refreshed_at: refreshedAt,
        next_refresh_at: nextRefreshAt,
        auto_refreshed: reason === "auto",
        fastapi_synced: sync.synced,
    }
}

async function autoRefreshDueTokens(user) {
    const now = new Date()
    const dueTokens = (user.event_tokens || []).filter((token) => {
        if (!token || token.is_active === false || token.auto_refresh_enabled === false) return false
        return getTokenNextRefreshDate(token) <= now
    })

    const refreshed = []
    for (const token of dueTokens) {
        const rotated = await rotateEventToken(user, token.event_type, "auto")
        if (rotated) refreshed.push(rotated)
    }
    return refreshed
}

function serializeEventToken(token) {
    const nextRefreshAt = getTokenNextRefreshDate(token)
    return {
        event_type: token.event_type,
        is_active: token.is_active,
        created_at: token.created_at,
        last_refreshed_at: token.last_refreshed_at || token.created_at,
        next_refresh_at: nextRefreshAt,
        expires_at: nextRefreshAt,
        auto_refresh_enabled: token.auto_refresh_enabled !== false,
    }
}

// -------------------- Middleware --------------------
function authenticate(req, res, next) {
    const authHeader = req.headers["authorization"]
    if (!authHeader) {
        return res.status(401).json({ message: "No token provided" })
    }
    const token = authHeader.split(" ")[1]
    jwt.verify(token, JWT_SECRET, (err, decoded) => {
        if (err) {
            return res.status(403).json({ message: "Invalid token" })
        }
        req.user = decoded
        next()
    })
}

// -------------------- REGISTER --------------------
app.post("/register", async (req, res) => {
    try {
        const { email, password, name, client_name } = req.body;

        const existingUser = await EmployeeModel.findOne({ email });

        // ❗ CASE: USER EXISTS
        if (existingUser) {
            return res.status(400).json({
                status: "exists",
                message: "Account already exists"
            });
        }

        const hashedPassword = await bcrypt.hash(password, 10);
        const client_id = generateClientId();

        const rawApiKey = generateApiKey();
        const hashedKey = await bcrypt.hash(rawApiKey, 10);
        const tokenCreatedAt = new Date();
        const tokenNextRefreshAt = addMonths(tokenCreatedAt);

        const company = await EmployeeModel.create({
            name,
            client_name,
            email,
            password: hashedPassword,
            client_id,
            event_tokens: [
                {
                    event_type: "DEFAULT",
                    api_key_hash: hashedKey,
                    is_active: true,
                    created_at: tokenCreatedAt,
                    last_refreshed_at: tokenCreatedAt,
                    next_refresh_at: tokenNextRefreshAt,
                    expires_at: tokenNextRefreshAt,
                    auto_refresh_enabled: true
                }
            ]
        });

        // 🔥 GENERATE JWT
        const token = jwt.sign(
            { email: company.email, id: company._id },
            JWT_SECRET,
            { expiresIn: "1h" }
        );

        // ── Sync this client to FastAPI so the API key works on /notify ──────
        // FastAPI stores its own clients collection using SHA-256 hash of the key.
        // We send the plain key once so FastAPI can hash and store it.
        // This is a fire-and-forget sync — registration succeeds even if FastAPI
        // is temporarily down (the user can re-sync later from the dashboard).
        try {
            await axios.post(`${FASTAPI_URL}/clients/sync`, {
                client_id:     client_id,
                name:          client_name || name,
                plain_api_key: rawApiKey,
                event_type:    "DEFAULT",
                allowed_channels: ["email", "sms", "whatsapp", "push"],
                monthly_quota: 10000000,
            });
            console.log(`[SYNC] Client ${client_id} synced to FastAPI`);
        } catch (syncErr) {
            // Log but don't fail the registration — user can retry sync later
            console.warn("[SYNC] FastAPI sync failed (non-fatal):", syncErr.message);
        }

        return res.status(201).json({
            status: "created",
            message: "Account created successfully",
            token,
            client_id,
            api_key: rawApiKey,   // nf_... key — shown ONCE, works in Swagger Authorize
        });

    } catch (err) {
        console.error("[REGISTER]", err)
        return res.status(500).json({
            message: "Server error"
        });
    }
});

// -------------------- LOGIN --------------------
app.post("/login", async (req, res) => {
    const { email, password } = req.body
    try {
        const user = await EmployeeModel.findOne({ email })
        if (!user) {
            return res.status(404).json({
                status: "error",
                message: "No account found"
            })
        }
        const isMatch = await bcrypt.compare(password, user.password)

        if (!isMatch) {
            return res.status(401).json({
                status: "error",
                message: "Incorrect password"
            })
        }
        const token = jwt.sign(
            {
                email: user.email,
                id: user._id
            },
            JWT_SECRET,
            { expiresIn: "1h" }
        )
        return res.json({
            status: "success",
            message: "Login successful",
            token
        })
    } catch (err) {
        return res.status(500).json({
            status: "error",
            message: "Server error"
        })
    }
});

// -------------------- HOME (Protected) -------------------
app.get("/home", authenticate, async (req, res) => {
    try {
        const user = await EmployeeModel.findOne({ email: req.user.email })
        if (!user) {
            return res.status(404).json({ message: "User not found" })
        }
        const autoRefreshedTokens = await autoRefreshDueTokens(user)
        const freshUser = autoRefreshedTokens.length
            ? await EmployeeModel.findOne({ email: req.user.email })
            : user
        res.json({
            client_id: freshUser.client_id,
            event_tokens: freshUser.event_tokens.map(serializeEventToken),
            auto_refreshed_tokens: autoRefreshedTokens,
            monthly_quota: freshUser.monthly_quota,
            quota_used: freshUser.quota_used
        })
    } catch (err) {
        console.error("HOME ERROR:", err)  
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- CREATE EVENT TOKEN --------------------
app.post("/create-token", authenticate, async (req, res) => {
    try {
        const eventType = normalizeEventType(req.body.event_type)
        if (!eventType) {
            return res.status(400).json({ message: "event_type required" })
        }
        if (eventType === "DEFAULT") {
            return res.status(400).json({ message: "DEFAULT token already exists. Use refresh/regenerate instead." })
        }
        const existing = await EmployeeModel.findOne({
            email: req.user.email,
            "event_tokens.event_type": eventType
        })
        if (existing) {
            return res.status(409).json({ message: `Token for ${eventType} already exists. Use refresh.` })
        }
        const rawKey = generateApiKey()
        const hash = await bcrypt.hash(rawKey, 10)
        const createdAt = new Date()
        const nextRefreshAt = addMonths(createdAt)
        await EmployeeModel.updateOne(
            { email: req.user.email },
            {
                $push: {
                    event_tokens: {
                        event_type: eventType,
                        api_key_hash: hash,
                        is_active: true,
                        created_at: createdAt,
                        last_refreshed_at: createdAt,
                        next_refresh_at: nextRefreshAt,
                        expires_at: nextRefreshAt,
                        auto_refresh_enabled: true
                    }
                }
            }
        )
        const user = await EmployeeModel.findOne({ email: req.user.email })
        await syncPlainApiKeyToFastApi(user, rawKey, eventType, "create-token")
        res.json({
            message: "Token created",
            event_type: eventType,
            api_key: rawKey,   // ⚠️ show only once
            next_refresh_at: nextRefreshAt
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- REFRESH TOKEN --------------------
app.post("/refresh-token", authenticate, async (req, res) => {
    try {
        const eventType = normalizeEventType(req.body.event_type)
        if (!eventType) {
            return res.status(400).json({ message: "event_type required" })
        }
        const user = await EmployeeModel.findOne({ email: req.user.email })
        if (!user) {
            return res.status(404).json({ message: "User not found" })
        }
        const refreshed = await rotateEventToken(user, eventType, "manual")
        if (!refreshed) {
            return res.status(404).json({ message: `No token found for event_type ${eventType}` })
        }
        res.json({
            message: "Token refreshed",
            ...refreshed
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- DISABLE TOKEN --------------------
app.post("/disable-token", authenticate, async (req, res) => {
    try {
        const eventType = normalizeEventType(req.body.event_type)
        if (!eventType) {
            return res.status(400).json({ message: "event_type required" })
        }
        const result = await EmployeeModel.updateOne(
            {
                email: req.user.email
            },
            {
                $set: {
                    "event_tokens.$[token].is_active": false
                }
            },
            {
                arrayFilters: [{ "token.event_type": eventType, "token.is_active": { $ne: false } }]
            },
        )
        if (result.modifiedCount === 0) {
            return res.status(404).json({ message: `No active token found for event_type ${eventType}` })
        }
        res.json({
            message: "Token disabled",
            event_type: eventType,
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- DELETE DISABLED TOKEN --------------------
app.post("/delete-disabled-token", authenticate, async (req, res) => {
    try {
        const eventType = normalizeEventType(req.body.event_type)
        if (!eventType) {
            return res.status(400).json({ message: "event_type required" })
        }
        const result = await EmployeeModel.updateOne(
            { email: req.user.email },
            {
                $pull: {
                    event_tokens: {
                        event_type: eventType,
                        is_active: false,
                    },
                },
            },
        )
        if (result.modifiedCount === 0) {
            return res.status(404).json({ message: `No disabled token found for event_type ${eventType}` })
        }
        res.json({
            message: "Disabled token deleted",
            event_type: eventType,
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
})

// -------------------- RESYNC TO FASTAPI --------------------
// Called from the dashboard "Sync to FastAPI" button if the initial
// sync during registration failed (e.g. FastAPI was down).
app.post("/sync-to-fastapi", authenticate, async (req, res) => {
    try {
        const user = await EmployeeModel.findOne({ email: req.user.email });
        if (!user) return res.status(404).json({ message: "User not found" });

        // Get the latest api_key from the request body (user pastes it)
        // or generate a new one if they want to reset
        const { plain_api_key } = req.body;
        if (!plain_api_key) {
            return res.status(400).json({ message: "plain_api_key is required" });
        }
        const event_type = req.body.event_type || "DEFAULT";

        await axios.post(`${FASTAPI_URL}/clients/sync`, {
            client_id:        user.client_id,
            name:             user.client_name || user.name,
            plain_api_key:    plain_api_key,
            event_type:       event_type,
            allowed_channels: ["email", "sms", "whatsapp", "push"],
            monthly_quota:    user.monthly_quota || 10000000,
        });

        res.json({ message: "Synced to FastAPI successfully.", client_id: user.client_id });
    } catch (err) {
        const detail = err.response?.data?.detail || err.message;
        res.status(500).json({ message: "Sync failed: " + detail });
    }
});

// -------------------- GENERATE NEW API KEY --------------------
// Creates a fresh API key, stores hash in MongoDB, syncs to FastAPI.
// Used by the "Regenerate API Key" button in the dashboard.
app.post("/generate-api-key", authenticate, async (req, res) => {
    try {
        const user = await EmployeeModel.findOne({ email: req.user.email });
        if (!user) return res.status(404).json({ message: "User not found" });

        const newRawKey = generateApiKey();            // nf_<64hex>
        const newHash   = await bcrypt.hash(newRawKey, 10);
        const refreshedAt = new Date();
        const nextRefreshAt = addMonths(refreshedAt);

        // Store hash in Node.js DB (replace DEFAULT token or add fresh one)
        await EmployeeModel.updateOne(
            { email: req.user.email, "event_tokens.event_type": "DEFAULT" },
            {
                $set: {
                    "event_tokens.$.api_key_hash": newHash,
                    "event_tokens.$.created_at": refreshedAt,
                    "event_tokens.$.last_refreshed_at": refreshedAt,
                    "event_tokens.$.next_refresh_at": nextRefreshAt,
                    "event_tokens.$.expires_at": nextRefreshAt,
                    "event_tokens.$.is_active": true,
                    "event_tokens.$.auto_refresh_enabled": true,
                }
            }
        );

        // Sync the new key to FastAPI by calling /clients/sync directly.
        // /clients/sync now upserts — it updates the hash if the client already
        // exists, so the new key immediately replaces the old one in FastAPI.
        await axios.post(`${FASTAPI_URL}/clients/sync`, {
            client_id:        user.client_id,
            name:             user.client_name || user.name,
            plain_api_key:    newRawKey,
            event_type:       "DEFAULT",
            allowed_channels: ["email", "sms", "whatsapp", "push"],
            monthly_quota:    user.monthly_quota || 10000000,
        });

        res.json({
            message: "API key regenerated. Save it now — shown only once.",
            client_id: user.client_id,
            api_key:   newRawKey,
            next_refresh_at: nextRefreshAt,
        });
    } catch (err) {
        res.status(500).json({ message: "Server error: " + err.message });
    }
});

// -------------------- DELETE MY ACCOUNT --------------------
app.delete("/delete-account", authenticate, async (req, res) => {
    try {
        const user = await EmployeeModel.findOne({ email: req.user.email });
        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }

        const clientId = user.client_id;
        const db = mongoose.connection.db;

        const mediaDocs = await db
            .collection("media_files")
            .find({ client_id: clientId }, { projection: { stored_path: 1 } })
            .toArray();

        let removedMediaFiles = 0;
        for (const doc of mediaDocs) {
            const path = doc?.stored_path;
            if (!path) continue;
            try {
                require("fs").unlinkSync(path);
                removedMediaFiles += 1;
            } catch (err) {
                if (err?.code !== "ENOENT") {
                    console.warn("[DELETE_ACCOUNT] media file cleanup failed:", err.message);
                }
            }
        }

        const deletedCounts = {
            api_clients: (await EmployeeModel.deleteOne({ email: req.user.email })).deletedCount,
            notification_requests: (await db.collection("notification_requests").deleteMany({ client_id: clientId })).deletedCount,
            notification_jobs: (await db.collection("notification_jobs").deleteMany({ client_id: clientId })).deletedCount,
            delivery_logs: (await db.collection("delivery_logs").deleteMany({ client_id: clientId })).deletedCount,
            rate_limit_logs: (await db.collection("rate_limit_logs").deleteMany({ client_id: clientId })).deletedCount,
            webhook_calls: (await db.collection("webhook_calls").deleteMany({ client_id: clientId })).deletedCount,
            user_preferences: (await db.collection("user_preferences").deleteMany({ client_id: clientId })).deletedCount,
            media_files: (await db.collection("media_files").deleteMany({ client_id: clientId })).deletedCount,
            dlq: (await db.collection("dlq").deleteMany({ client_id: clientId })).deletedCount,
            media_files_removed_from_disk: removedMediaFiles,
        };

        return res.json({
            message: "Your account has been deleted.",
            client_id: clientId,
            deleted_counts: deletedCounts,
        });
    } catch (err) {
        console.error("[DELETE_ACCOUNT]", err);
        return res.status(500).json({ message: "Server error" });
    }
});

// -------------------- START SERVER --------------------
app.listen(3001, () => {
    console.log("Server is running on port 3001")
})
