import { useState, useEffect } from "react"
import axios from "axios"
import { useNavigate } from "react-router-dom"
import "./Auth.css"

function Home() {
    const [clientId, setClientId] = useState("")
    const [tokens, setTokens] = useState([])
    const [tokenApiKeys, setTokenApiKeys] = useState({})
    const [newEvent, setNewEvent] = useState("")
    const [newApiKey, setNewApiKey] = useState("")
    const [loading, setLoading] = useState(true)
    const [activeTab, setActiveTab] = useState("home")
    const [jobs, setJobs] = useState([])
    const [search, setSearch] = useState("")   
    const [failedJobs, setFailedJobs] = useState([])
    useEffect(() => {
        fetch("http://localhost:8000/dlq")
            .then(res => res.json())
            .then(data => {
                console.log("DLQ API response:", data)
                setFailedJobs(toSafeObjectArray(data?.dlq))
            })
            .catch(err => {
                console.error("DLQ fetch error:", err)
                setFailedJobs([])
            })
    }, [])
    const [failStats, setFailStats] = useState([])
    const [failureSummary, setFailureSummary] = useState(null)
    const [webhooks, setWebhooks] = useState([])
    const [analyticsFromDate, setAnalyticsFromDate] = useState("")
    const [analyticsToDate, setAnalyticsToDate] = useState("")
    const [webhookStatusFilter, setWebhookStatusFilter] = useState("")
    const [webhookEventFilter, setWebhookEventFilter] = useState("")
    const [queueStats, setQueueStats] = useState({
        global: {
            total: 0,
            waiting: 0,
            active: 0,
            failed: 0
        },
        queues: []
    })
    const [selectedQueue, setSelectedQueue] = useState("EMAIL")
    const [rateLimit, setRateLimit] = useState(50)
    const [selectedJob, setSelectedJob] = useState(null);
    const [clients, setClients] = useState([])
    const [clientSearch, setClientSearch] = useState("")
    const [clientStatusFilter, setClientStatusFilter] = useState("all")
    const [quotaDrafts, setQuotaDrafts] = useState({})
    const toSafeObjectArray = (value) =>
        Array.isArray(value) ? value.filter((item) => item && typeof item === "object") : []
    const toDisplayText = (value, fallback = "-") => {
        if (value === null || value === undefined || value === "") return fallback
        if (typeof value === "object") {
            if (typeof value.message === "string" && value.message) return value.message
            try {
                return JSON.stringify(value)
            } catch {
                return String(value)
            }
        }
        return String(value)
    }
    const matchesDlqQueueFilter = (job, filterValue) => {
        if (!filterValue) return true
        const normalized = String(filterValue).trim().toLowerCase()
        const channel = String(job.channel || "").trim().toLowerCase()
        const queueName = String(job.queue_name || "").trim().toLowerCase()
        if (normalized === "email") return channel === "email" || queueName === "email-notify-q"
        if (normalized === "sms") return channel === "sms" || queueName === "sms-notify-q"
        if (normalized === "whatsapp") return channel === "whatsapp" || queueName === "whatsapp-notify-q"
        if (normalized === "push") return channel === "push" || queueName === "push-notify-q"
        return channel === normalized || queueName === normalized
    }
    const [tokenModal, setTokenModal] = useState({
        open: false,
        eventType: "",
        apiKey: "",
    })
    const viewJob = (job) => {
        setSelectedJob(job);
    };
    const fetchDLQ = async () => {
        try {
            const res = await axios.get("http://localhost:8000/dlq", getAuthHeader())
            setFailedJobs(toSafeObjectArray(res.data?.dlq))
        } catch (err) {
            console.log("DLQ refresh error:", err)
            setFailedJobs([])
        }
    }
    const fetchJobs = async () => {
        try {
            const res = await axios.get(
                "http://localhost:8000/jobs",
                {
                    params: { client_id: clientId || undefined, limit: 200 },
                    ...getAuthHeader(),
                }
            )
            setJobs(toSafeObjectArray(res.data?.jobs))
        } catch (err) {
            console.log("Jobs fetch error:", err)
        }
    }
    useEffect(() => {
        if (activeTab === "jobs") {
            fetchJobs()
        }
    }, [activeTab, clientId])
    const navigate = useNavigate()
    const handleTabChange = (tab) => {
        setActiveTab(tab)
        localStorage.setItem("client_dashboard_active_tab", tab)
    }
    const getAuthHeader = () => {
        const token = localStorage.getItem("token")
        return {
            headers: {
                Authorization: `Bearer ${token}`
            }
        }
    }
    const fetchData = async () => {
        try {
            const res = await axios.get(
                "http://localhost:3001/home",
                getAuthHeader()
            )

            const fetchedClientId = res.data.client_id
            setClientId(fetchedClientId)
            setTokens(toSafeObjectArray(res.data?.event_tokens))
            const storedTokenKeys = localStorage.getItem(`tokenApiKeys_${fetchedClientId}`)
            if (storedTokenKeys) {
                try {
                    setTokenApiKeys(JSON.parse(storedTokenKeys))
                } catch {
                    // Prevent malformed local storage from crashing the dashboard render.
                    setTokenApiKeys({})
                }
            } else {
                setTokenApiKeys({})
            }

        } catch (err) {
            console.log("Auth error:", err.response?.data || err.message)
            localStorage.removeItem("token")
        } finally {
            setLoading(false)
        }
    }
    useEffect(() => {
        const token = localStorage.getItem("token")
        if (!token) {
            navigate("/login")
            return
        }
        const savedTab = localStorage.getItem("client_dashboard_active_tab")
        const validTabs = new Set([
            "home", "stats", "control", "dlq", "tokens", "jobs",
            "logs", "rate", "analytics", "webhooks", "clients"
        ])
        if (savedTab && validTabs.has(savedTab)) setActiveTab(savedTab)
        fetchData()
        fetchQueueStats()
    }, [])
    const handleLogout = () => {
        localStorage.removeItem("token")
        navigate("/login")
    }

    // ---------------- API KEY FUNCTIONS ----------------
    const saveTokenApiKey = (eventType, apiKey, explicitClientId = null) => {
        const cid = explicitClientId || clientId
        if (!cid || !eventType || !apiKey) return
        const normalizedEvent = String(eventType).trim().toUpperCase()
        const next = { ...tokenApiKeys, [normalizedEvent]: apiKey }
        setTokenApiKeys(next)
        localStorage.setItem(`tokenApiKeys_${cid}`, JSON.stringify(next))
    }

    // Calls Node.js /generate-api-key → Node.js syncs to FastAPI → returns nf_ key
    const generateNewApiKey = async () => {
        try {
            const res = await axios.post(
                "http://localhost:3001/generate-api-key",
                {},
                getAuthHeader()
            )
            setNewApiKey(res.data.api_key)
            setClientId(res.data.client_id)
            saveTokenApiKey("DEFAULT", res.data.api_key, res.data.client_id)
            fetchData()
        } catch (err) {
            alert(err.response?.data?.message || "Failed to generate API key")
        }
    }

    // Copies text and shows brief confirmation
    const [copied, setCopied] = useState(false)
    const copyToClipboard = async (text) => {
        await navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
    }

    // Legacy createToken kept for the "My Tokens" tab backward compat
    const createToken = async () => {
        if (!newEvent.trim()) return alert("Enter event type")
        try {
            const res = await axios.post(
                "http://localhost:3001/create-token",
                { event_type: newEvent.trim().toUpperCase() },
                getAuthHeader()
            )
            setNewApiKey(res.data.api_key)
            saveTokenApiKey(res.data.event_type || newEvent, res.data.api_key)
            setTokenModal({
                open: true,
                eventType: res.data.event_type || newEvent.trim().toUpperCase(),
                apiKey: res.data.api_key,
            })
            setNewEvent("")
            fetchData()
        } catch (err) {
            alert(err.response?.data?.message || "Failed to create token")
        }
    }

    const refreshToken = async (event_type) => {
        try {
            const res = await axios.post(
                "http://localhost:3001/refresh-token",
                { event_type },
                getAuthHeader()
            )
            saveTokenApiKey(res.data.event_type || event_type, res.data.api_key)
            setTokenModal({
                open: true,
                eventType: res.data.event_type || event_type,
                apiKey: res.data.api_key,
            })

        } catch (err) {
            alert("Failed to refresh token")
        }
    }

    const disableToken = async (event_type) => {
        try {
            await axios.post(
                "http://localhost:3001/disable-token",
                { event_type },
                getAuthHeader()
            )

            fetchData()

        } catch (err) {
            alert(err.response?.data?.message || "Failed to disable token")
        }
    }

    const deleteDisabledToken = async (event_type) => {
        try {
            await axios.post(
                "http://localhost:3001/delete-disabled-token",
                { event_type },
                getAuthHeader()
            )
            fetchData()
        } catch (err) {
            alert(err.response?.data?.message || "Failed to delete disabled token")
        }
    }

    // ---------------- QUEUE CONTROLS (MOCK) ----------------
    // const handlePause = () => alert("Queue Paused (mock)")
    // const handleResume = () => alert("Queue Resumed (mock)")
    // const handleKill = () => alert("Queue Killed (mock)")

      /* Queue Stats Backend Integration  */
    const fetchQueueStats = async () => {
        try {
            const res = await axios.get(
                "http://localhost:8000/queue-stats",
                getAuthHeader()
            )
            console.log("QUEUE STATS:", res.data)
            setQueueStats({
                global: {
                    total: Number(res.data?.global?.total || 0),
                    waiting: Number(res.data?.global?.waiting || 0),
                    active: Number(res.data?.global?.active || 0),
                    failed: Number(res.data?.global?.failed || 0),
                },
                queues: toSafeObjectArray(res.data?.queues),
            })

        } catch (err) {
            console.log("Queue stats error:", err)
        }
    }
    
    const handlePause = async () => {
        await axios.post("http://localhost:8000/pause-queue", {
            queue: selectedQueue
        }, getAuthHeader())
    }
    const handleResume = async () => {
        await axios.post("http://localhost:8000/resume-queue", {
            queue: selectedQueue
        }, getAuthHeader())
    }
    const handleKill = async () => {
        await axios.post("http://localhost:8000/clear-queue", {
            queue: selectedQueue
        }, getAuthHeader())
    }
    const retryFailed = async () => {
        await axios.post("http://localhost:8000/retry-failed", {}, getAuthHeader())
    }
    const reprocessDLQ = async () => {
        await axios.post("http://localhost:8000/reprocess-dlq", {}, getAuthHeader())
    }
    const updateRate = async () => {
        await axios.post("http://localhost:8000/update-rate-limit", {
            queue: selectedQueue,
            rate: rateLimit
        }, getAuthHeader())
    }
    const pauseQueue = async () => {
        try {
            const res = await axios.post(
                "http://localhost:8000/pause-queue",
                { queue: selectedQueue },
                getAuthHeader()
            )

            alert(res.data.message)   // 👈 THIS shows "EMAIL paused"

        } catch (err) {
            console.log(err)
            alert("Pause failed")
        }
    }
    const resumeQueue = async () => {
        try {
            const res = await axios.post(
                "http://localhost:8000/resume-queue",
                { queue: selectedQueue },
                getAuthHeader()
            )

            alert(res.data.message)

        } catch (err) {
            alert("Resume failed")
        }
    }
    const clearQueue = async () => {
        try {
            const res = await axios.post(
                "http://localhost:8000/clear-queue",
                { queue: selectedQueue },
                getAuthHeader()
            )

            alert(`${res.data.message} (${res.data.tasks_removed} removed)`)

        } catch (err) {
            alert("Clear failed")
        }
    }
    const updateRateLimit = async () => {
        try {
            const res = await axios.post(
                "http://localhost:8000/update-rate-limit",
                {
                    queue: selectedQueue,
                    rate: rateLimit
                },
                getAuthHeader()
            )

            alert(res.data.message)

        } catch (err) {
            alert("Update failed")
        }
    }
    const retryJob = async (jobId) => {
        try {
            await axios.post("http://localhost:8000/retry-job", {
                job_id: jobId
            }, getAuthHeader())

            fetchDLQ()  // refresh
        } catch (err) {
            console.log(err)
        }
    }
    const discardJob = async (jobId) => {
        try {
            await axios.post("http://localhost:8000/discard-job", {
                job_id: jobId
            }, getAuthHeader())

            fetchDLQ()
        } catch (err) {
            console.log(err)
        }
    }
    const [logs, setLogs] = useState([])
    const [logFilter, setLogFilter] = useState("ALL")
    const [logSearch, setLogSearch] = useState("")
    const fetchLogs = async () => {
        try {
            const res = await axios.get(
                "http://localhost:8000/logs",
                {
                    params: {
                        limit: 500,
                    },
                    ...getAuthHeader(),
                }
            )

            console.log("LOGS:", res.data)

            const sorted = [...toSafeObjectArray(res.data?.logs)].sort(
                (a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0)
            )
            setLogs(sorted)
        } catch (err) {
            console.log("Logs fetch error:", err)
        }
    }
    useEffect(() => {
        if (activeTab === "logs") {
            fetchLogs()
        }
    }, [activeTab])

    const fetchClients = async () => {
        try {
            const res = await axios.get("http://localhost:8000/clients", getAuthHeader())
            const docs = toSafeObjectArray(res.data?.clients)
            setClients(docs)
            const nextDrafts = {}
            for (const c of docs) {
                nextDrafts[c.client_id] = c.monthly_quota ?? 100000
            }
            setQuotaDrafts(nextDrafts)
        } catch (err) {
            console.log("Clients fetch error:", err)
            setClients([])
        }
    }

    const toggleClientActive = async (client) => {
        try {
            await axios.patch(
                `http://localhost:8000/clients/${client.client_id}`,
                { is_active: !client.is_active },
                getAuthHeader()
            )
            fetchClients()
        } catch (err) {
            alert(err.response?.data?.detail || "Failed to update client status")
        }
    }

    const updateClientQuota = async (client) => {
        try {
            const quota = Number(quotaDrafts[client.client_id])
            if (!Number.isFinite(quota) || quota <= 0) {
                return alert("Enter a valid quota")
            }
            await axios.patch(
                `http://localhost:8000/clients/${client.client_id}`,
                { monthly_quota: quota },
                getAuthHeader()
            )
            fetchClients()
        } catch (err) {
            alert(err.response?.data?.detail || "Failed to update monthly quota")
        }
    }

    const rotateClientKey = async (client) => {
        try {
            const res = await axios.post(
                `http://localhost:8000/clients/${client.client_id}/rotate-key`,
                {},
                getAuthHeader()
            )
            setTokenModal({
                open: true,
                eventType: `CLIENT ${client.client_id} (ROTATED KEY)`,
                apiKey: res.data.api_key,
            })
        } catch (err) {
            alert(err.response?.data?.detail || "Failed to rotate API key")
        }
    }

    useEffect(() => {
        if (activeTab === "clients") {
            fetchClients()
        }
    }, [activeTab])

    const [rateLogs, setRateLogs] = useState([])
    const [filterClient, setFilterClient] = useState("")
    const [filterChannel, setFilterChannel] = useState("")
    const [filterScope, setFilterScope] = useState("")
    const [rateLimitLimit, setRateLimitLimit] = useState(50)
    const fetchRateLogs = async () => {
        try {
            const res = await axios.get("http://localhost:8000/rate-limit-logs", {
                params: {
                    client_id: filterClient || undefined,
                    channel: filterChannel || undefined,
                    scope: filterScope || undefined,
                    limit: rateLimitLimit || 50,
                },
                ...getAuthHeader()
            })

            console.log("RATE LOGS:", res.data)
            setRateLogs(toSafeObjectArray(res.data?.rate_limit_logs))
        } catch (err) {
            console.log("Rate logs error:", err)
        }
    }
    useEffect(() => {
        if (activeTab === "rate") {
            fetchRateLogs()
        }
    }, [activeTab, filterClient, filterChannel, filterScope, rateLimitLimit])

    const fetchFailureAnalytics = async () => {
        try {
            const [statsRes, failedLogsRes] = await Promise.all([
                axios.get("http://localhost:8000/stats", {
                    params: {
                        client_id: clientId || undefined,
                        from_date: analyticsFromDate || undefined,
                        to_date: analyticsToDate || undefined,
                    },
                    ...getAuthHeader(),
                }),
                axios.get("http://localhost:8000/logs", {
                    params: {
                        client_id: clientId || undefined,
                        status: "FAILED",
                        from_date: analyticsFromDate || undefined,
                        to_date: analyticsToDate || undefined,
                        limit: 500,
                    },
                    ...getAuthHeader(),
                }),
            ])

            const failedByEvent = {}
            for (const row of toSafeObjectArray(failedLogsRes.data?.logs)) {
                const key = row.event_type || "UNKNOWN"
                failedByEvent[key] = (failedByEvent[key] || 0) + 1
            }
            const rows = Object.entries(failedByEvent)
                .map(([event, count]) => ({ event, count }))
                .sort((a, b) => b.count - a.count)

            setFailStats(rows)
            setFailureSummary(statsRes.data.summary || null)
        } catch (err) {
            console.log("Failure analytics error:", err)
            setFailStats([])
            setFailureSummary(null)
        }
    }

    useEffect(() => {
        if (activeTab === "analytics") {
            fetchFailureAnalytics()
        }
    }, [activeTab, clientId, analyticsFromDate, analyticsToDate])

    const fetchWebhookLogs = async () => {
        try {
            const res = await axios.get("http://localhost:8000/logs", {
                params: {
                    client_id: clientId || undefined,
                    channel: "email",
                    status: webhookStatusFilter || undefined,
                    event_type: webhookEventFilter || undefined,
                    limit: 100,
                },
                ...getAuthHeader(),
            })
            setWebhooks(toSafeObjectArray(res.data?.logs))
        } catch (err) {
            console.log("Webhook logs error:", err)
            setWebhooks([])
        }
    }

    useEffect(() => {
        if (activeTab === "webhooks") {
            fetchWebhookLogs()
        }
    }, [activeTab, clientId, webhookStatusFilter, webhookEventFilter])
    
    if (loading) {
        return (
            <div className="auth-container">
                <div className="auth-card">
                    <h3>Loading...</h3>
                </div>
            </div>
        )
    }

    return (
        <div style={{ display: "flex", height: "100vh" }}>
            {/* -------- SIDEBAR -------- */}
            <div style={{
                width: "220px",
                background: "#1e293b",
                color: "white",
                padding: "20px"
            }}>
            <h3
                style={{ cursor: "pointer" }}
                onClick={() => handleTabChange("home")}>
                Dashboard
            </h3>

            <div className={`sidebar-item ${activeTab === "stats" ? "active" : ""}`} onClick={() => handleTabChange("stats")}>
                Queue Stats
            </div>

            <div className={`sidebar-item ${activeTab === "control" ? "active" : ""}`} onClick={() => handleTabChange("control")}>
                Queue Control
            </div>

            <div className={`sidebar-item ${activeTab === "dlq" ? "active" : ""}`} onClick={() => handleTabChange("dlq")}>
                Dead Letter Queue
            </div>

            <div className={`sidebar-item ${activeTab === "tokens" ? "active" : ""}`} onClick={() => handleTabChange("tokens")}>
                My Tokens
            </div>

            <div className={`sidebar-item ${activeTab === "jobs" ? "active" : ""}`} onClick={() => handleTabChange("jobs")}>
                Job Explorer
            </div>

            <button
                type="button"
                className={`sidebar-item ${activeTab === "logs" ? "active" : ""}`}
                onClick={() => {
                    handleTabChange("logs")
                    fetchLogs()
                }}
                style={{ width: "100%", textAlign: "left", border: "none", color: "inherit", background: "transparent" }}
            >
                Logs / Activity
            </button>

            <div className={`sidebar-item ${activeTab === "rate" ? "active" : ""}`} onClick={() => handleTabChange("rate")}>
                Rate Limits
            </div>

            <div className={`sidebar-item ${activeTab === "analytics" ? "active" : ""}`} onClick={() => handleTabChange("analytics")}>
                Failure Analytics
            </div>

            <button
                type="button"
                className={`sidebar-item ${activeTab === "webhooks" ? "active" : ""}`}
                onClick={() => {
                    handleTabChange("webhooks")
                    fetchWebhookLogs()
                }}
                style={{ width: "100%", textAlign: "left", border: "none", color: "inherit", background: "transparent" }}
            >
                Webhook Logs
            </button>

            <div className={`sidebar-item ${activeTab === "clients" ? "active" : ""}`} onClick={() => handleTabChange("clients")} >
                Client Monitor
            </div>

            <button onClick={handleLogout} style={{ marginTop: "20px" }}>
                Logout
            </button>
        </div>
        
        {/* -------- HOME (DASHBOARD) -------- */}
        {activeTab === "home" && (
            <>
                <div className="home-container">

                    {/* ── Client Identity ── */}
                    <div className="client-id-box" style={{
                        background: "#0f172a",
                        border: "1px solid #334155",
                        borderRadius: "10px",
                        padding: "16px 20px",
                        marginBottom: "24px",
                        display: "flex",
                        alignItems: "center",
                        gap: "12px"
                    }}>
                        <span style={{ color: "#94a3b8", fontSize: "13px", fontFamily: "monospace" }}>CLIENT ID</span>
                        <span style={{
                            color: "#e2e8f0",
                            fontFamily: "monospace",
                            fontSize: "14px",
                            background: "#1e293b",
                            padding: "4px 12px",
                            borderRadius: "6px",
                            letterSpacing: "0.05em"
                        }}>{clientId || "—"}</span>
                        {clientId && (
                            <button
                                onClick={() => copyToClipboard(clientId)}
                                style={{
                                    background: "none", border: "1px solid #475569",
                                    color: "#94a3b8", borderRadius: "5px",
                                    padding: "2px 10px", fontSize: "12px", cursor: "pointer"
                                }}
                            >
                                {copied ? "✓" : "Copy"}
                            </button>
                        )}
                    </div>

                    {/* ── Generate API Key Card ── */}
                    <div style={{
                        background: "#0f172a",
                        border: "1px solid #334155",
                        borderRadius: "10px",
                        padding: "24px",
                        marginBottom: "24px",
                    }}>
                        <h4 style={{ color: "#f1f5f9", marginBottom: "8px" }}>🔑 API Key for Swagger / Postman</h4>
                        <p style={{ color: "#64748b", fontSize: "13px", marginBottom: "20px", lineHeight: "1.6" }}>
                            Generate an API key to authenticate requests. Paste it into
                            <strong style={{ color: "#94a3b8" }}> Swagger → Authorize 🔒</strong> or your
                            HTTP client's <code style={{ color: "#7dd3fc" }}>X-API-Key</code> header.
                        </p>

                        <button
                            onClick={generateNewApiKey}
                            style={{
                                background: "linear-gradient(135deg, #3b82f6, #6366f1)",
                                color: "white",
                                border: "none",
                                borderRadius: "8px",
                                padding: "10px 22px",
                                fontWeight: "600",
                                fontSize: "14px",
                                cursor: "pointer",
                                letterSpacing: "0.02em"
                            }}
                        >
                            ✦ Generate New API Key
                        </button>
                    </div>
                </div>

                {/* ── API Key Reveal Banner ── */}
                {newApiKey && (
                    <div style={{
                        margin: "20px 0",
                        background: "linear-gradient(135deg, #0c1a0c, #0f2a0f)",
                        border: "1px solid #166534",
                        borderRadius: "12px",
                        padding: "20px 24px",
                    }}>
                        <div style={{
                            display: "flex", alignItems: "center", gap: "8px",
                            marginBottom: "14px"
                        }}>
                            <span style={{ fontSize: "18px" }}>⚠️</span>
                            <strong style={{ color: "#86efac", fontSize: "14px" }}>
                                Save this API Key — shown only once and never stored in plain text
                            </strong>
                        </div>

                        <div style={{ display: "flex", gap: "10px", alignItems: "center" }}>
                            <input
                                type="text"
                                value={newApiKey}
                                readOnly
                                style={{
                                    flex: 1,
                                    fontFamily: "monospace",
                                    fontSize: "13px",
                                    background: "#052e16",
                                    border: "1px solid #166534",
                                    color: "#4ade80",
                                    borderRadius: "7px",
                                    padding: "10px 14px",
                                    letterSpacing: "0.05em"
                                }}
                            />
                            <button
                                onClick={() => copyToClipboard(newApiKey)}
                                style={{
                                    background: copied ? "#166534" : "#15803d",
                                    color: "white",
                                    border: "none",
                                    borderRadius: "7px",
                                    padding: "10px 18px",
                                    fontWeight: "600",
                                    cursor: "pointer",
                                    minWidth: "90px",
                                    transition: "background 0.2s"
                                }}
                            >
                                {copied ? "✓ Copied" : "Copy"}
                            </button>
                        </div>

                        <div style={{
                            marginTop: "14px",
                            background: "#052e16",
                            borderRadius: "7px",
                            padding: "12px 16px",
                            fontSize: "13px",
                            color: "#86efac",
                            fontFamily: "monospace",
                            lineHeight: "1.7"
                        }}>
                            <div style={{ color: "#4ade80", marginBottom: "6px", fontWeight: "600" }}>
                                Quick copy for Swagger or curl:
                            </div>
                            <div>Client ID: <span style={{ color: "#bbf7d0" }}>{clientId}</span></div>
                            <div>X-API-Key: <span style={{ color: "#bbf7d0" }}>{newApiKey}</span></div>
                            <div style={{ marginTop: "8px", color: "#6ee7b7", fontSize: "12px" }}>
                                curl -X POST http://localhost:8000/notify \<br/>
                                &nbsp;&nbsp;-H "X-API-Key: {newApiKey}" \<br/>
                                &nbsp;&nbsp;-H "Content-Type: application/json" \<br/>
                                &nbsp;&nbsp;-d &#123;"client_id":"{clientId}", ...&#125;
                            </div>
                        </div>
                    </div>
                )}
            </>
        )}

        {/* -------- MAIN CONTENT -------- */}
        <div style={{ flex: 1, padding: "20px" }}>

            {/* -------- QUEUE STATS -------- */}
            {activeTab === "stats" && (
                <>
                    <h2>Queue Stats</h2>

                    {/* -------- GLOBAL STATS -------- */}
                    <div className="stats-grid">
                        <div className="card">
                            <p>Total Jobs</p>
                            <h3>{queueStats.global.total}</h3>
                        </div>

                        <div className="card">
                            <p>Waiting</p>
                            <h3>{queueStats.global.waiting}</h3>
                        </div>

                        <div className="card">
                            <p>Active</p>
                            <h3>{queueStats.global.active}</h3>
                        </div>

                        <div className="card">
                            <p>Failed</p>
                            <h3>{queueStats.global.failed}</h3>
                        </div>
                    </div>

                    {/* -------- PER QUEUE TABLE -------- */}
                    <h3 className="mt-3">Per Queue Breakdown</h3>

                    <table>
                        <thead>
                            <tr>
                                <th>Queue</th>
                                <th>Waiting</th>
                                <th>Active</th>
                                <th>Failed</th>
                            </tr>
                        </thead>

                        <tbody>
                            {queueStats.queues.map((q, i) => (
                                <tr key={i}>
                                    <td>{q.type}</td>
                                    <td>{q.waiting}</td>
                                    <td>{q.active}</td>
                                    <td>{q.failed}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </>
            )}

            {activeTab === "control" && (
                <div className="control-container">
                    
                    {/* -------- QUEUE CONTROL -------- */}
                    <h2>Queue Control</h2>

                    {/* Queue Selector */}
                    <div className="form-group">
                        <label>Select Queue</label>
                        <select
                            value={selectedQueue}
                            onChange={(e) => setSelectedQueue(e.target.value)}
                            className="form-control"
                        >
                            <option value="EMAIL">EMAIL</option>
                            <option value="WHATSAPP">WHATSAPP</option>
                            <option value="SMS">SMS</option>
                        </select>
                    </div>

                    {/* Buttons */}
                    <div className="control-buttons">
                        <button className="btn btn-warning" onClick={pauseQueue}>Pause</button>
                        <button className="btn btn-success" onClick={resumeQueue}>Resume</button>
                        <button className="btn btn-danger" onClick={clearQueue}>Clear Queue</button>
                    </div>

                    <div className="control-buttons">
                        <button className="btn btn-primary">Retry Failed</button>
                        <button className="btn btn-secondary">Reprocess DLQ</button>
                    </div>

                    {/* Rate Limit */}
                    <div className="rate-limit-box">
                        <label>Rate Limit (jobs/sec)</label>

                        <div className="rate-input-group">
                            <input
                                type="number"
                                value={rateLimit}
                                onChange={(e) => setRateLimit(e.target.value)}
                                className="form-control"
                            />
                            <button className="btn btn-update" onClick={updateRateLimit}>Update</button>
                        </div>
                    </div>
                </div>
            )}

            {/* -------- DLQ -------- */}
            {activeTab === "dlq" && (
                <>
                    <h2>Dead Letter Queue</h2>
                    {/* Queue Filter */}
                    <div className="dlq-filter">
                        <label>Filter by Queue:</label>
                        <select
                            value={selectedQueue}
                            onChange={(e) => setSelectedQueue(e.target.value)}
                        >
                            <option value="">All</option>
                            <option value="EMAIL">EMAIL</option>
                            <option value="SMS">SMS</option>
                        </select>
                    </div>

                    <table border="1" cellPadding="10">
                        <thead>
                            <tr>
                                <th>Job ID</th>
                                <th>Queue</th>
                                <th>Error</th>
                                <th>Type</th>
                                <th>Retries</th>
                                <th>Failed At</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {failedJobs
                                .filter(job => matchesDlqQueueFilter(job, selectedQueue))
                                .map(job => (
                                    <tr key={job.job_id}>
                                        <td>{job.job_id}</td>
                                        <td>{job.queue_name || job.channel || "-"}</td>

                                        {/* Truncated error */}
                                        <td title={toDisplayText(job.error_message, "")}>
                                            {toDisplayText(job.error_message).length > 30
                                                ? toDisplayText(job.error_message).substring(0, 30) + "..."
                                                : toDisplayText(job.error_message)}
                                        </td>
                                        {/* Error Type */}
                                        <td>
                                            <span
                                                style={{
                                                    color: job.error_type === "TRANSIENT" ? "orange" : "red",
                                                    fontWeight: "bold"
                                                }}
                                            >
                                                {job.error_type}
                                            </span>
                                        </td>

                                        <td>{job.retry_count}</td>
                                        <td>
                                            {job.failed_at
                                                ? new Date(job.failed_at).toLocaleString()
                                                : "-"}
                                        </td>
                                        <td>
                                            {/* Retry only for transient */}
                                            <button
                                                onClick={() => retryJob(job.job_id)}
                                                disabled={job.error_type !== "TRANSIENT"}
                                            >
                                                Retry
                                            </button>
                                            <button
                                                onClick={() => discardJob(job.job_id)}
                                                style={{ marginLeft: "5px" }}
                                            >
                                                Discard
                                            </button>
                                            <button
                                                onClick={() => viewJob(job)}
                                                style={{ marginLeft: "5px" }}
                                            >
                                                View
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                        </tbody>
                    </table>
                </>
            )}

            {/* -------- TOKENS -------- */}
            {activeTab === "tokens" && (
                <>
                    <h2>My Tokens</h2>
                    <div
                        style={{
                            background: "#0f172a",
                            border: "1px solid #334155",
                            borderRadius: "8px",
                            padding: "10px 12px",
                            marginBottom: "12px",
                            display: "flex",
                            alignItems: "center",
                            gap: "10px",
                            flexWrap: "wrap",
                        }}
                    >
                        <span style={{ color: "#94a3b8", fontSize: "12px", letterSpacing: "0.04em" }}>
                            CLIENT ID
                        </span>
                        <span
                            style={{
                                color: "#e2e8f0",
                                background: "#1e293b",
                                borderRadius: "6px",
                                padding: "4px 10px",
                                fontFamily: "monospace",
                                fontSize: "13px",
                            }}
                        >
                            {clientId || "—"}
                        </span>
                        {clientId && (
                            <button
                                onClick={() => copyToClipboard(clientId)}
                                style={{ background: "#334155", color: "#fff" }}
                            >
                                Copy
                            </button>
                        )}
                    </div>
                    <div style={{ display: "flex", gap: "8px", marginBottom: "14px", alignItems: "center" }}>
                        <input
                            type="text"
                            placeholder="Enter event type (e.g., PROMO)"
                            value={newEvent}
                            onChange={(e) => setNewEvent(e.target.value)}
                            style={{ width: "520px", maxWidth: "100%" }}
                        />
                        <button onClick={createToken}>Create Token</button>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Event Type</th>
                                <th>API Key</th>
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>

                        <tbody>
                            {tokens.map((t, i) => (
                                <tr key={i}>
                                    <td>{t.event_type}</td>
                                    <td style={{ fontFamily: "monospace", maxWidth: "340px", wordBreak: "break-all" }}>
                                        {tokenApiKeys[t.event_type] || "Not available (shown only when generated/refreshed)"}
                                    </td>

                                    <td>
                                        <span
                                            style={{
                                                color: t.is_active ? "green" : "red",
                                                fontWeight: "bold"
                                            }}
                                        >
                                            {t.is_active ? "Active" : "Disabled"}
                                        </span>
                                    </td>

                                    <td>
                                        <button
                                            onClick={() => refreshToken(t.event_type)}
                                            disabled={!t.is_active}
                                        >
                                            Refresh
                                        </button>

                                        <button
                                            onClick={() => disableToken(t.event_type)}
                                            disabled={!t.is_active}
                                            style={{ marginLeft: "5px" }}
                                        >
                                            Disable
                                        </button>
                                        {!t.is_active && (
                                            <button
                                                onClick={() => deleteDisabledToken(t.event_type)}
                                                style={{ marginLeft: "5px", background: "#ef4444", color: "#fff" }}
                                            >
                                                Delete
                                            </button>
                                        )}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </>
            )}
            
            {/* -------- JOB EXPLORER -----------*/ }
            {activeTab === "jobs" && (
                <>
                    <h2>Job Explorer</h2>

                    {/* 🔽 Channel Filter */}
                    <div className="job-filter">
                        <label>Filter by Channel:</label>
                        <select
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                        >
                            <option value="">All</option>
                            <option value="EMAIL">EMAIL</option>
                            <option value="SMS">SMS</option>
                            <option value="PUSH">PUSH</option>
                            <option value="WHATSAPP">WHATSAPP</option>
                        </select>
                    </div>

                    {/* 📋 Job Table */}
                    <table>
                        <thead>
                            <tr>
                                <th>Job ID</th>
                                <th>Event</th>
                                <th>Channel</th>
                                <th>Status</th>
                                <th>Created At</th>
                            </tr>
                        </thead>

                        <tbody>
                            {jobs
                                .filter(j => !search || String(j.channel || "").toUpperCase() === search)
                                .map(job => (
                                    <tr key={job.job_id}>
                                        <td>{job.job_id}</td>
                                        <td>{job.event_type}</td>
                                        <td>{job.channel}</td>
                                        <td>
                                            <span className={`status ${job.status?.toLowerCase()}`}>
                                                {job.status}
                                            </span>
                                        </td>
                                        <td>
                                            {job.created_at
                                                ? new Date(job.created_at).toLocaleString()
                                                : "-"}
                                        </td>
                                    </tr>
                                ))}
                        </tbody>
                    </table>
                </>
            )}

            {/* ----------------- LOGS / ACTIVITY ------------------*/}
            {activeTab === "logs" && (
                <>
                    <h2>Logs / Activity</h2>
                    <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap" }}>
                        <select value={logFilter} onChange={(e) => setLogFilter(e.target.value)}>
                            <option value="ALL">All Statuses</option>
                            <option value="SENT">SENT</option>
                            <option value="DELIVERED">DELIVERED</option>
                            <option value="READ">READ</option>
                            <option value="FAILED">FAILED</option>
                            <option value="SPAM">SPAM</option>
                            <option value="DLQ">DLQ</option>
                        </select>
                        <input
                            type="text"
                            placeholder="Search by job id / event type / client id"
                            value={logSearch}
                            onChange={(e) => setLogSearch(e.target.value)}
                            style={{ width: "360px", maxWidth: "100%" }}
                        />
                    </div>
                    {/* 📋 Logs Table */}
                    <table>
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Event</th>
                                <th>Status</th>
                                <th>Channel</th>
                                <th>Client ID</th>
                                <th>User ID</th>
                                <th>Job ID</th>
                                <th>Error</th>
                            </tr>
                        </thead>

                        <tbody>
                            {logs
                                .filter(log =>
                                    (logFilter === "ALL" || log.status === logFilter) &&
                                    `${log.job_id || ""} ${log.event_type || ""} ${log?.job?.client_id || ""}`
                                        .toLowerCase()
                                        .includes(logSearch.toLowerCase())
                                )
                                .map((log, i) => (
                                    <tr key={i}>
                                        <td>
                                            {log.created_at
                                                ? new Date(log.created_at).toLocaleString()
                                                : "-"}
                                        </td>

                                        <td>{log.event_type || "-"}</td>
                                        <td>{log.status || "-"}</td>
                                        <td>{log.channel || "-"}</td>
                                        <td>{log?.job?.client_id || "-"}</td>
                                        <td>{log?.job?.recipient_user_id || "-"}</td>
                                        <td>{log.job_id || "-"}</td>
                                        <td>{toDisplayText(log.error)}</td>
                                    </tr>
                                ))}
                        </tbody>
                    </table>
                </>
            )}

            {/*---------------------------------- RATE LIMITS -----------------------------------*/}
            {activeTab === "rate" && (
                <>
                    <h2>Rate Limit Dashboard</h2>
                    <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap" }}>
                        <input
                            type="text"
                            placeholder="Client ID"
                            value={filterClient}
                            onChange={(e) => setFilterClient(e.target.value)}
                        />
                        <select value={filterChannel} onChange={(e) => setFilterChannel(e.target.value)}>
                            <option value="">All Channels</option>
                            <option value="email">email</option>
                            <option value="sms">sms</option>
                            <option value="whatsapp">whatsapp</option>
                            <option value="push">push</option>
                        </select>
                        <select value={filterScope} onChange={(e) => setFilterScope(e.target.value)}>
                            <option value="">All Scopes</option>
                            <option value="user">user</option>
                            <option value="client">client</option>
                            <option value="global_ip">global_ip</option>
                            <option value="global_system">global_system</option>
                        </select>
                        <input
                            type="number"
                            min="1"
                            max="200"
                            value={rateLimitLimit}
                            onChange={(e) => setRateLimitLimit(Number(e.target.value || 50))}
                            style={{ maxWidth: "120px" }}
                            title="Result limit"
                        />
                    </div>
                    <table>
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Client ID</th>
                            <th>User ID</th>
                            <th>Channel</th>
                            <th>Scope</th>
                            <th>Layer</th>
                            <th>Limit</th>
                            <th>Count</th>
                            <th>Retry After</th>
                        </tr>
                    </thead>

                    <tbody>
                        {rateLogs.map((log, i) => (
                            <tr key={i}>
                                <td>{log.blocked_at ? new Date(log.blocked_at).toLocaleString() : "-"}</td>
                                <td>{log.client_id || "-"}</td>
                                <td>{log.user_id || "-"}</td>
                                <td>{log.channel || "-"}</td>
                                <td>{log.scope || "-"}</td>
                                <td>{log.layer || "-"}</td>
                                <td>{log.limit || "-"}</td>
                                <td>{log.count || "-"}</td>
                                <td>{log.retry_after_seconds || "-"}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
                </>
            )}

            {activeTab === "analytics" && (
                <>
                    <h2>Failure Analytics</h2>
                    <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap" }}>
                        <input
                            type="date"
                            value={analyticsFromDate}
                            onChange={(e) => setAnalyticsFromDate(e.target.value)}
                        />
                        <input
                            type="date"
                            value={analyticsToDate}
                            onChange={(e) => setAnalyticsToDate(e.target.value)}
                        />
                    </div>
                    {failureSummary && (
                        <div style={{ marginBottom: "12px", color: "#334155" }}>
                            <strong>Total:</strong> {failureSummary.total || 0}{" "}
                            | <strong>Failure Rate:</strong> {failureSummary.failure_rate_pct || 0}%{" "}
                            | <strong>Delivery Rate:</strong> {failureSummary.delivery_rate_pct || 0}%
                        </div>
                    )}

                    <table>
                        <thead>
                            <tr>
                                <th>Event Type</th>
                                <th>Failures</th>
                            </tr>
                        </thead>
                        <tbody>
                            {failStats.map((f, i) => (
                                <tr key={i}>
                                    <td>{f.event}</td>
                                    <td>{f.count}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </>
            )}

            {activeTab === "webhooks" && (
                <>
                    <h2>Webhook Logs</h2>
                    <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap" }}>
                        <input
                            type="text"
                            placeholder="Filter by event type"
                            value={webhookEventFilter}
                            onChange={(e) => setWebhookEventFilter(e.target.value)}
                        />
                        <select
                            value={webhookStatusFilter}
                            onChange={(e) => setWebhookStatusFilter(e.target.value)}
                        >
                            <option value="">All Statuses</option>
                            <option value="SENT">SENT</option>
                            <option value="DELIVERED">DELIVERED</option>
                            <option value="READ">READ</option>
                            <option value="FAILED">FAILED</option>
                            <option value="SPAM">SPAM</option>
                        </select>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Log ID</th>
                                <th>Event</th>
                                <th>Status</th>
                                <th>Provider</th>
                                <th>Provider Message ID</th>
                                <th>Created At</th>
                                <th>Error</th>
                            </tr>
                        </thead>
                        <tbody>
                            {webhooks.map(w => (
                                <tr key={w.log_id}>
                                    <td>{w.log_id}</td>
                                    <td>{w.event_type}</td>
                                    <td>{w.status}</td>
                                    <td>{w.provider || "-"}</td>
                                    <td>{w.provider_message_id || "-"}</td>
                                    <td>{w.created_at ? new Date(w.created_at).toLocaleString() : "-"}</td>
                                    <td>{toDisplayText(w.error)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </>
            )}

            {activeTab === "clients" && (
                <>
                    <h2>Client Monitor</h2>
                    <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap" }}>
                        <input
                            type="text"
                            placeholder="Search by client id or name"
                            value={clientSearch}
                            onChange={(e) => setClientSearch(e.target.value)}
                            style={{ width: "280px", maxWidth: "100%" }}
                        />
                        <select
                            value={clientStatusFilter}
                            onChange={(e) => setClientStatusFilter(e.target.value)}
                        >
                            <option value="all">All</option>
                            <option value="active">Active</option>
                            <option value="inactive">Inactive</option>
                        </select>
                        <button onClick={fetchClients}>Refresh</button>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Client ID</th>
                                <th>Name</th>
                                <th>Status</th>
                                <th>Monthly Quota</th>
                                <th>Allowed Channels</th>
                                <th>Created At</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {clients
                                .filter((c) => {
                                    const byStatus =
                                        clientStatusFilter === "all" ||
                                        (clientStatusFilter === "active" && c.is_active) ||
                                        (clientStatusFilter === "inactive" && !c.is_active)
                                    const q = clientSearch.trim().toLowerCase()
                                    const bySearch =
                                        !q ||
                                        String(c.client_id || "").toLowerCase().includes(q) ||
                                        String(c.name || "").toLowerCase().includes(q)
                                    return byStatus && bySearch
                                })
                                .map((c) => (
                                    <tr key={c.client_id}>
                                        <td>{c.client_id}</td>
                                        <td>{c.name || "-"}</td>
                                        <td>
                                            <span style={{ color: c.is_active ? "green" : "red", fontWeight: "bold" }}>
                                                {c.is_active ? "Active" : "Inactive"}
                                            </span>
                                        </td>
                                        <td>
                                            <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
                                                <input
                                                    type="number"
                                                    min="1"
                                                    value={quotaDrafts[c.client_id] ?? c.monthly_quota ?? ""}
                                                    onChange={(e) =>
                                                        setQuotaDrafts((prev) => ({
                                                            ...prev,
                                                            [c.client_id]: e.target.value,
                                                        }))
                                                    }
                                                    style={{ width: "110px" }}
                                                />
                                                <button onClick={() => updateClientQuota(c)}>Save</button>
                                            </div>
                                        </td>
                                        <td>{(c.allowed_channels || []).join(", ") || "-"}</td>
                                        <td>{c.created_at ? new Date(c.created_at).toLocaleString() : "-"}</td>
                                        <td>
                                            <button onClick={() => toggleClientActive(c)}>
                                                {c.is_active ? "Deactivate" : "Activate"}
                                            </button>
                                            <button
                                                onClick={() => rotateClientKey(c)}
                                                style={{ marginLeft: "6px" }}
                                            >
                                                Rotate Key
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                        </tbody>
                    </table>
                </>
            )}
        </div>

        {tokenModal.open && (
            <div
                style={{
                    position: "fixed",
                    inset: 0,
                    background: "rgba(2, 6, 23, 0.75)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    zIndex: 1000,
                    padding: "16px",
                }}
            >
                <div
                    style={{
                        width: "100%",
                        maxWidth: "620px",
                        background: "#0f172a",
                        border: "1px solid #334155",
                        borderRadius: "12px",
                        padding: "20px",
                        color: "#e2e8f0",
                    }}
                >
                    <h3 style={{ marginTop: 0, marginBottom: "10px" }}>New API Key Generated</h3>
                    <p style={{ marginTop: 0, marginBottom: "12px", color: "#94a3b8", fontSize: "14px" }}>
                        Event Type: <strong style={{ color: "#f8fafc" }}>{tokenModal.eventType}</strong>
                    </p>
                    <p style={{ marginTop: 0, marginBottom: "14px", color: "#fca5a5", fontSize: "13px" }}>
                        Save this key now. It is shown only once.
                    </p>
                    <input
                        type="text"
                        readOnly
                        value={tokenModal.apiKey}
                        style={{
                            width: "100%",
                            background: "#020617",
                            border: "1px solid #334155",
                            color: "#7dd3fc",
                            borderRadius: "8px",
                            padding: "10px 12px",
                            fontFamily: "monospace",
                            marginBottom: "14px",
                        }}
                    />
                    <div style={{ display: "flex", justifyContent: "flex-end", gap: "8px" }}>
                        <button
                            onClick={() => copyToClipboard(tokenModal.apiKey)}
                            style={{ background: "#2563eb", color: "#fff" }}
                        >
                            Copy Key
                        </button>
                        <button
                            onClick={() => setTokenModal({ open: false, eventType: "", apiKey: "" })}
                            style={{ background: "#334155", color: "#fff" }}
                        >
                            Close
                        </button>
                    </div>
                </div>
            </div>
        )}
    </div>
)
}
export default Home