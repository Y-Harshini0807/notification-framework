const express = require("express")
const mongoose = require("mongoose")
const cors = require("cors")
const bcrypt = require("bcrypt")
const jwt = require("jsonwebtoken")
const crypto = require("crypto")
const JWT_SECRET = "your_secret_key"
const EmployeeModel = require("./models/Employee")

const app = express()
app.use(express.json())
app.use(cors())

mongoose.connect("mongodb://localhost:27017/employee")
const EmployeeSchema = new mongoose.Schema({
    name: String,
    client_name: String,
    email: String,
    password: String,
    client_id: String,

    event_tokens: {
        type: Array,
        default: []   // 🔥 important
    },

    monthly_quota: {
        type: Number,
        default: 1000
    },

    quota_used: {
        type: Number,
        default: 0
    }
});

// -------------------- Utility Functions --------------------
function generateClientId() {
    return crypto.randomBytes(16).toString("hex")
}

function generateApiKey() {
    return crypto.randomBytes(32).toString("hex")
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

        // 🔥 GENERATE JWT HERE
        const token = jwt.sign(
            { email: company.email, id: company._id },
            JWT_SECRET,
            { expiresIn: "1h" }
        );

        return res.status(201).json({
            status: "created",
            message: "Account created successfully",
            token,            // 🔥 IMPORTANT
            client_id,
            api_key: rawApiKey
        });

    } catch (err) {
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
        const { event_type } = req.body
        if (!event_type) {
            return res.status(400).json({ message: "event_type required" })
        }
        const rawKey = generateApiKey()
        const hash = await bcrypt.hash(rawKey, 10)
        await EmployeeModel.updateOne(
            { email: req.user.email },
            {
                $push: {
                    event_tokens: {
                        event_type,
                        api_key_hash: hash,
                        is_active: true,
                        created_at: new Date(),
                        expires_at: null
                    }
                }
            }
        )
        res.json({
            message: "Token created",
            event_type,
            api_key: rawKey   // ⚠️ show only once
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- REFRESH TOKEN --------------------
app.post("/refresh-token", authenticate, async (req, res) => {
    try {
        const { event_type } = req.body
        const rawKey = generateApiKey()
        const hash = await bcrypt.hash(rawKey, 10)

        await EmployeeModel.updateOne(
            {
                email: req.user.email,
                "event_tokens.event_type": event_type
            },
            {
                $set: {
                    "event_tokens.$.api_key_hash": hash,
                    "event_tokens.$.created_at": new Date()
                }
            }
        )
        res.json({
            message: "Token refreshed",
            event_type,
            api_key: rawKey
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- DISABLE TOKEN --------------------
app.post("/disable-token", authenticate, async (req, res) => {
    try {
        const { event_type } = req.body
        await EmployeeModel.updateOne(
            {
                email: req.user.email,
                "event_tokens.event_type": event_type
            },
            {
                $set: {
                    "event_tokens.$.is_active": false
                }
            }
        )
        res.json({
            message: "Token disabled"
        })
    } catch (err) {
        res.status(500).json({ message: "Server error" })
    }
});

// -------------------- START SERVER --------------------
app.listen(3001, () => {
    console.log("Server is running on port 3001")
})

mongoose.Schema