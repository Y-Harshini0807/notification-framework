const express = require("express")
const axios   = require("axios")
const mongoose = require("mongoose")
const cors = require("cors")
const bcrypt = require("bcrypt")
const jwt = require("jsonwebtoken")
const crypto = require("crypto")
const JWT_SECRET   = "your_secret_key"
const FASTAPI_URL  = process.env.FASTAPI_URL || "http://localhost:8000"
const EmployeeModel = require("./models/client")

const app = express()
app.use(express.json())
app.use(cors())

mongoose.connect("mongodb://localhost:27017/notification_db")

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
                    created_at: new Date()
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
                monthly_quota: 100000,
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
        res.json({
            client_id: user.client_id,
            event_tokens: user.event_tokens.map(t => ({
                event_type: t.event_type,
                is_active: t.is_active,
                created_at: t.created_at
            })),
            monthly_quota: user.monthly_quota,
            quota_used: user.quota_used
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
        await EmployeeModel.updateOne(
            { email: req.user.email },
            {
                $push: {
                    event_tokens: {
                        event_type: eventType,
                        api_key_hash: hash,
                        is_active: true,
                        created_at: new Date(),
                        expires_at: null
                    }
                }
            }
        )
        const user = await EmployeeModel.findOne({ email: req.user.email })
        try {
            await axios.post(`${FASTAPI_URL}/clients/sync`, {
                client_id:        user.client_id,
                name:             user.client_name || user.name,
                plain_api_key:    rawKey,
                event_type:       eventType,
                allowed_channels: ["email", "sms", "whatsapp", "push"],
                monthly_quota:    user.monthly_quota || 100000,
            })
            console.log(`[SYNC] create-token → FastAPI for ${user.client_id}`)
        } catch (syncErr) {
            console.warn("[SYNC] FastAPI sync failed (non-fatal):", syncErr.message)
        }
        res.json({
            message: "Token created",
            event_type: eventType,
            api_key: rawKey   // ⚠️ show only once
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
        const rawKey = generateApiKey()
        const hash = await bcrypt.hash(rawKey, 10)

        const result = await EmployeeModel.updateOne(
            {
                email: req.user.email,
                "event_tokens.event_type": eventType
            },
            {
                $set: {
                    "event_tokens.$.api_key_hash": hash,
                    "event_tokens.$.created_at": new Date(),
                    "event_tokens.$.is_active": true
                }
            }
        )
        if (result.matchedCount === 0) {
            return res.status(404).json({ message: `No token found for event_type ${eventType}` })
        }
        const user = await EmployeeModel.findOne({ email: req.user.email })
        try {
            await axios.post(`${FASTAPI_URL}/clients/sync`, {
                client_id:        user.client_id,
                name:             user.client_name || user.name,
                plain_api_key:    rawKey,
                event_type:       eventType,
                allowed_channels: ["email", "sms", "whatsapp", "push"],
                monthly_quota:    user.monthly_quota || 100000,
            })
            console.log(`[SYNC] refresh-token → FastAPI for ${user.client_id}`)
        } catch (syncErr) {
            console.warn("[SYNC] FastAPI sync failed (non-fatal):", syncErr.message)
        }
        res.json({
            message: "Token refreshed",
            event_type: eventType,
            api_key: rawKey
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
            monthly_quota:    user.monthly_quota || 100000,
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

        // Store hash in Node.js DB (replace DEFAULT token or add fresh one)
        await EmployeeModel.updateOne(
            { email: req.user.email, "event_tokens.event_type": "DEFAULT" },
            { $set: { "event_tokens.$.api_key_hash": newHash, "event_tokens.$.created_at": new Date() } }
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
            monthly_quota:    user.monthly_quota || 100000,
        });

        res.json({
            message: "API key regenerated. Save it now — shown only once.",
            client_id: user.client_id,
            api_key:   newRawKey,
        });
    } catch (err) {
        res.status(500).json({ message: "Server error: " + err.message });
    }
});

// -------------------- START SERVER --------------------
app.listen(3001, () => {
    console.log("Server is running on port 3001")
})
