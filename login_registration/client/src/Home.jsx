import { useState, useEffect } from "react"
import axios from "axios"
import { useNavigate } from "react-router-dom"
import "./Auth.css"
import ProviderSettings from "./ProviderSettings"

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
    const [failStats, setFailStats] = useState([])
    const [failureSummary, setFailureSummary] = useState(null)
    const [webhooks, setWebhooks] = useState([])
    const [analyticsFromDate, setAnalyticsFromDate] = useState(() => {
    const d = new Date()
              d.setDate(d.getDate() - 30)
              return d.toISOString().slice(0, 10)
     })
    const [analyticsToDate, setAnalyticsToDate] = useState(() => {
              return new Date().toISOString().slice(0, 10)
     })
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
    const [showQueueControl, setShowQueueControl] = useState(false)
    const [queueControlChannel, setQueueControlChannel] = useState(null) // null = closed, "EMAIL"|"SMS"|etc = open
    // Rate limit draft for the queue control modal.
    const [rateLimitDraft, setRateLimitDraft] = useState("0")
    const [rateLimitTouched, setRateLimitTouched] = useState(false)
    const [selectedClients, setSelectedClients] = useState(new Set())
    const [clientSelectMode, setClientSelectMode] = useState(false)
    const [selectedJob, setSelectedJob] = useState(null)
    const [activityDetail, setActivityDetail] = useState(null)
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
    const selectedQueueKey = String(selectedQueue || "").trim().toUpperCase()
    const selectedQueueStats = queueStats.queues.find(
        (q) => String(q.type || "").trim().toUpperCase() === selectedQueueKey
    ) || null
    const getQueueRateLimitDraft = (queueType) => {
        const normalized = String(queueType || "").trim().toUpperCase()
        const queueStatsEntry = queueStats.queues.find(
            (q) => String(q.type || "").trim().toUpperCase() === normalized
        ) || null
        return String(Number(queueStatsEntry?.rate_limit || 0))
    }
    useEffect(() => {
        if (!queueControlChannel || rateLimitTouched) return
        setRateLimitDraft(getQueueRateLimitDraft(queueControlChannel))
    }, [queueControlChannel, queueStats.queues, rateLimitTouched])
    const parseBackendTimestamp = (value, assumedOffset = "") => {
        if (!value) return null
        if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value
        if (typeof value === "number") {
            const date = new Date(value)
            return Number.isNaN(date.getTime()) ? null : date
        }
        if (typeof value !== "string") return null

        const normalized = value.trim().replace(" ", "T")
        const hasTimezone = /(?:Z|[+-]\d{2}:\d{2})$/i.test(normalized)
        const candidate = hasTimezone ? normalized : `${normalized}${assumedOffset}`
        const date = new Date(candidate)
        return Number.isNaN(date.getTime()) ? null : date
    }
    const formatAdminTimestamp = (value, assumedOffset = "") => {
        if (!value) return "-"
        const date = parseBackendTimestamp(value, assumedOffset)
        if (!date) return String(value)
        return new Intl.DateTimeFormat(undefined, {
            dateStyle: "medium",
            timeStyle: "medium",
            timeZone: "Asia/Kolkata",
        }).format(date)
    }
    const formatUtcTimestamp = (value) => formatAdminTimestamp(value, "Z")
    const SUCCESS_ACTIVITY_STATUSES = new Set(["SENT", "DELIVERED", "READ"])
    const FAILED_ACTIVITY_STATUSES = new Set(["FAILED", "DLQ", "DROPPED", "SPAM"])
    const formatDateTime = (value) => {
        if (!value) return "-"
        const date = new Date(value)
        if (Number.isNaN(date.getTime())) return String(value)
        return date.toLocaleString()
    }
    const renderFailoverReasons = (reasons) => {
        const rows = Array.isArray(reasons) ? reasons.filter(Boolean) : []
        if (!rows.length) return "Healthy"
        return rows.join(", ")
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
        setSelectedJob(job)
    }
    const openActivityDetail = (title, userIds, emptyMessage) => {
        setActivityDetail({
            title,
            userIds: Array.isArray(userIds) ? userIds.filter(Boolean) : [],
            emptyMessage,
        })
    }
    const renderActivityLink = (label, onClick) => (
        <button
            type="button"
            onClick={onClick}
            style={{
                background: "none",
                border: "none",
                padding: 0,
                color: "#2563eb",
                cursor: "pointer",
                textDecoration: "underline",
                font: "inherit",
            }}
        >
            {label}
        </button>
    )
    const fetchDLQ = async () => {
        try {
            const res = await axios.get("http://localhost:8000/dlq", {
                params: { status: "DLQ", limit: 200 },
                ...getAuthHeader(),
            })
            setFailedJobs(toSafeObjectArray(res.data?.dlq))
        } catch (err) {
            console.log("DLQ refresh error:", err)
            setFailedJobs([])
        }
    }
    useEffect(() => {
        fetchDLQ()
    }, [])
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
        if (activeTab === "jobs" || activeTab === "activity") {
            fetchJobs()
        }
    }, [activeTab, clientId])
    useEffect(() => {
        if (activeTab === "dlq") {
            fetchDLQ()
        }
    }, [activeTab])
    const navigate = useNavigate()
    const handleTabChange = (tab) => {
        if (tab !== "stats") {
            setShowQueueControl(false)
            setQueueControlChannel(null)
        }
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
            "home", "stats", "dlq", "tokens", "activity", "rate", "analytics", "webhooks", "clients", "providers"
        ])
        if (savedTab === "control") {
            setActiveTab("stats")
        } else if (savedTab && validTabs.has(savedTab)) {
            setActiveTab(savedTab)
        }
        fetchData()
        fetchQueueStats()
    }, [])
    useEffect(() => {
        if (activeTab !== "stats") return
        fetchQueueStats()
        const id = setInterval(fetchQueueStats, 5000)
        return () => clearInterval(id)
    }, [activeTab])
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
    const [copiedTarget, setCopiedTarget] = useState("")
    const copyToClipboard = async (text, target = "default") => {
        await navigator.clipboard.writeText(text)
        setCopiedTarget(target)
        setTimeout(() => setCopiedTarget((current) => (current === target ? "" : current)), 2000)
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
        await axios.post(
            "http://localhost:8000/update-rate-limit",
            {
                queue: selectedQueue,
                rate: Number(rateLimitDraft),
            },
            getAuthHeader()
        )
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
                    rate: Number(rateLimitDraft)
                },
                getAuthHeader()
            )

            alert(res.data.message)
            setRateLimitTouched(false)
            setRateLimitDraft(String(Number(rateLimitDraft || 0)))
            fetchQueueStats()

        } catch (err) {
            alert("Update failed")
        }
    }
    const openQueueControl = (queueType) => {
        const normalized = String(queueType || "EMAIL").trim().toUpperCase()
        setSelectedQueue(normalized)
        setQueueControlChannel(normalized)
        setShowQueueControl(true)
        setActiveTab("stats")
        setRateLimitTouched(false)
        setRateLimitDraft(getQueueRateLimitDraft(normalized))
    }
    const retryJob = async (jobId) => {
        try {
            const res = await axios.post("http://localhost:8000/retry-job", {
                job_id: jobId
            }, getAuthHeader())

            alert(res.data?.message || "Job requeued")
            fetchDLQ()  // refresh
            return true
        } catch (err) {
            console.log(err)
            alert(err.response?.data?.detail || "Retry failed")
            return false
        }
    }
    const discardJob = async (jobId) => {
        try {
            const res = await axios.post("http://localhost:8000/discard-job", {
                job_id: jobId
            }, getAuthHeader())

            alert(res.data?.message || "Job discarded")
            fetchDLQ()
            return true
        } catch (err) {
            console.log(err)
            alert(err.response?.data?.detail || "Discard failed")
            return false
        }
    }
    const discardAllDlq = async () => {
        const visibleCount = failedJobs.filter((job) => matchesDlqQueueFilter(job, selectedQueue)).length
        if (!visibleCount) {
            return alert("No DLQ jobs to discard")
        }

        const scopeLabel = selectedQueue ? `${selectedQueue} DLQ jobs` : "all DLQ jobs"
        const confirmed = window.confirm(
            `Discard ${visibleCount} ${scopeLabel}? This will mark them as DROPPED.`
        )
        if (!confirmed) return

        try {
            const res = await axios.post(
                "http://localhost:8000/discard-dlq",
                { channel: selectedQueue || null },
                getAuthHeader()
            )
            alert(res.data?.message || "DLQ jobs discarded")
            setSelectedJob(null)
            fetchDLQ()
        } catch (err) {
            console.log(err)
            alert(err.response?.data?.detail || "Discard all failed")
        }
    }
    const [logs, setLogs] = useState([])

    // Merged Jobs + Activity filters
    const [filterJobId, setFilterJobId] = useState("")
    const [filterChannel2, setFilterChannel2] = useState("")
    const [filterEventType2, setFilterEventType2] = useState("")
    const [filterClientId2, setFilterClientId2] = useState("")
    const [filterStatus2, setFilterStatus2] = useState("")
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

            const sorted = [...toSafeObjectArray(res.data?.logs)].sort((a, b) => {
                const left = parseBackendTimestamp(a.created_at, "+05:30")?.getTime() ?? 0
                const right = parseBackendTimestamp(b.created_at, "+05:30")?.getTime() ?? 0
                return right - left
            })
            setLogs(sorted)
        } catch (err) {
            console.log("Logs fetch error:", err)
        }
    }
    useEffect(() => {
        if (activeTab === "activity") {
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

    const deleteClientAccount = async (client) => {
        const label = client?.client_id || "this client"
        const confirmed = window.confirm(
            `Delete client account ${label}? This will remove the client record and related notification data.`
        )
        if (!confirmed) return

        try {
            const res = await axios.delete(
                `http://localhost:8000/clients/${client.client_id}`,
                getAuthHeader()
            )
            alert(res.data.message || "Client deleted")
            fetchClients()
        } catch (err) {
            alert(err.response?.data?.detail || "Failed to delete client")
        }
    }
    const deleteAllClientAccounts = async () => {
        if (!clients.length) {
            return alert("No client accounts to delete")
        }

        const confirmation = window.prompt(
            `Delete all ${clients.length} client account(s) and their related notification data? Type DELETE ALL to confirm.`
        )
        if (confirmation !== "DELETE ALL") return

        try {
            const res = await axios.delete(
                "http://localhost:8000/clients",
                getAuthHeader()
            )
            alert(res.data?.message || "Client accounts deleted")
            setQuotaDrafts({})
            fetchClients()
        } catch (err) {
            alert(err.response?.data?.detail || "Failed to delete client accounts")
        }
    }
    const deleteSelectedClients = async () => {
            if (!selectedClients.size) return alert("No clients selected")
            const confirmed = window.confirm(
                `Delete ${selectedClients.size} selected client(s)? This cannot be undone.`
            )
            if (!confirmed) return
            try {
                await Promise.all(
                    [...selectedClients].map(id =>
                        axios.delete(`http://localhost:8000/clients/${id}`, getAuthHeader())
                    )
                )
                setSelectedClients(new Set())
                fetchClients()
            } catch (err) {
                alert(err.response?.data?.detail || "Failed to delete some clients")
            }
    }
    const deleteMyAccount = async () => {
        const confirmed = window.confirm(
            "Delete your admin account? This will remove your account, tokens, and related notification data from this service."
        )
        if (!confirmed) return

        try {
            const res = await axios.delete(
                "http://localhost:3001/delete-account",
                getAuthHeader()
            )
            alert(res.data.message || "Your account has been deleted")
            localStorage.removeItem("token")
            navigate("/login")
        } catch (err) {
            alert(err.response?.data?.message || "Failed to delete your account")
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
        const res = await axios.get("http://localhost:8000/logs", {
            params: { limit: 500 },
            ...getAuthHeader(),
        })

        const allLogs = toSafeObjectArray(res.data?.logs)

        // Apply date filter on frontend using IST offset (+05:30)
        const from = analyticsFromDate ? new Date(analyticsFromDate + "T00:00:00+05:30") : null
        const to   = analyticsToDate   ? new Date(analyticsToDate   + "T23:59:59+05:30") : null

        const filtered = allLogs.filter(log => {
            if (!from && !to) return true
            const d = parseBackendTimestamp(log.created_at, "+05:30")
            if (!d) return true
            if (from && d < from) return false
            if (to   && d > to)   return false
            return true
        })

        const failedLogs = filtered.filter(log =>
            String(log.status || "").toUpperCase() === "FAILED"
        )

        const failedByEvent = {}
        for (const row of failedLogs) {
            const key = row.event_type || "UNKNOWN"
            failedByEvent[key] = (failedByEvent[key] || 0) + 1
        }
        const rows = Object.entries(failedByEvent)
            .map(([event, count]) => ({ event, count }))
            .sort((a, b) => b.count - a.count)

        const total  = filtered.length
        const failed = failedLogs.length
        const sent   = total - failed

        setFailStats(rows)
        setFailureSummary(
            total > 0
                ? {
                    total,
                    failure_rate_pct:  ((failed / total) * 100).toFixed(1),
                    delivery_rate_pct: ((sent   / total) * 100).toFixed(1),
                }
                : { total: 0, failure_rate_pct: 0, delivery_rate_pct: 0 }
        )
    } catch (err) {
        console.log("Failure analytics ERROR:", err)
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
        <div style={{ display: "flex", minHeight: "100vh", alignItems: "flex-start" }}>
            {/* -------- SIDEBAR -------- */}
            <div style={{
                width: "220px",
                minWidth: "220px",
                background: "#1e293b",
                color: "white",
                padding: "20px",
                position: "sticky",
                top: 0,
                height: "100vh",
                overflowY: "auto",
                boxSizing: "border-box",
            }}>
            <h3
                style={{ cursor: "pointer" }}
                onClick={() => handleTabChange("home")}>
                Dashboard
            </h3>

            <div className={`sidebar-item ${activeTab === "stats" ? "active" : ""}`} onClick={() => handleTabChange("stats")}>
                Queue Stats
            </div>

            <div className={`sidebar-item ${activeTab === "dlq" ? "active" : ""}`} onClick={() => handleTabChange("dlq")}>
                Dead Letter Queue
            </div>

            <div className={`sidebar-item ${activeTab === "tokens" ? "active" : ""}`} onClick={() => handleTabChange("tokens")}>
                My Tokens
            </div>

            <div className={`sidebar-item ${activeTab === "activity" ? "active" : ""}`} onClick={() => handleTabChange("activity")}>
                Jobs + Activity
            </div>

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
            
            <div className={`sidebar-item ${activeTab === "providers" ? "active" : ""}`} onClick={() => handleTabChange("providers")}>
                Provider Settings
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
                                onClick={() => copyToClipboard(clientId, "home-client-id")}
                                style={{
                                    background: "none", border: "1px solid #475569",
                                    color: "#94a3b8", borderRadius: "5px",
                                    padding: "2px 10px", fontSize: "12px", cursor: "pointer"
                                }}
                            >
                                {copiedTarget === "home-client-id" ? "✓" : "Copy"}
                            </button>
                        )}
                    </div>

                    <div style={{
                        background: "#fff1f2",
                        border: "1px solid #fecdd3",
                        borderRadius: "10px",
                        padding: "16px 20px",
                        marginBottom: "24px",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        gap: "14px",
                        flexWrap: "wrap",
                    }}>
                        <div>
                            <div style={{ color: "#9f1239", fontWeight: 700, marginBottom: "4px" }}>Danger Zone</div>
                            <div style={{ color: "#881337", fontSize: "14px" }}>
                                Delete your admin account and remove its related data from this service.
                            </div>
                        </div>
                        <button
                            onClick={deleteMyAccount}
                            style={{
                                background: "#be123c",
                                color: "#fff",
                                border: "none",
                                borderRadius: "8px",
                                padding: "10px 14px",
                                fontWeight: 700,
                                cursor: "pointer",
                            }}
                        >
                            Delete My Account
                        </button>
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
                                onClick={() => copyToClipboard(newApiKey, "home-api-key")}
                                style={{
                                    background: copiedTarget === "home-api-key" ? "#166534" : "#15803d",
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
                                {copiedTarget === "home-api-key" ? "✓ Copied" : "Copy"}
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
        <div style={{ flex: 1, padding: "20px", minWidth: 0, boxSizing: "border-box" }}>

            {/* -------- QUEUE STATS -------- */}
            {activeTab === "stats" && (
                <>
                    <h2>Queue Stats</h2>
                    <div style={{ marginBottom: "16px" }}>
                        <button className="btn btn-primary" onClick={fetchQueueStats}>Refresh Queue Stats</button>
                    </div>

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
                                <th>Primary Waiting</th>
                                <th>Backup Waiting</th>
                                <th>Waiting</th>
                                <th>Active</th>
                                <th>Failed</th>
                                <th>Active Queue</th>
                                <th>Dynamic Backup</th>
                                <th>Status</th>
                                <th>Action</th>
                            </tr>
                        </thead>

                        <tbody>
                            {queueStats.queues.map((q, i) => (
                                <tr key={i}>
                                    <td>{q.type}</td>
                                    <td>{q.waiting}</td>
                                    <td>{Number(q.backup_waiting || 0)}</td>
                                    <td>{Number(q.waiting || 0) + Number(q.backup_waiting || 0)}</td>
                                    <td>{q.active}</td>
                                    <td>{q.failed}</td>
                                    <td style={{ maxWidth: "220px", wordBreak: "break-word" }}>{q.active_queue || "-"}</td>
                                    <td style={{ maxWidth: "220px", wordBreak: "break-word" }}>{q.backup_queue || "Not created"}</td>
                                    <td>{renderFailoverReasons(q.failover_reasons)}</td>
                                    <td>
                                        <button
                                            type="button"
                                            className="btn btn-primary"
                                            onClick={() => openQueueControl(q.type)}
                                        >
                                            View
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                    {/* Queue Control Modal — rendered as fixed overlay, scoped to clicked channel */}
                    {queueControlChannel && (
                        <div
                            style={{
                                position: "fixed",
                                inset: 0,
                                background: "rgba(0,0,0,0.35)",
                                display: "flex",
                                alignItems: "center",
                                justifyContent: "center",
                                zIndex: 1000,
                                padding: "16px",
                            }}
                            onClick={(e) => {
                                // Close on backdrop click
                                if (e.target === e.currentTarget) {
                                    setQueueControlChannel(null)
                                    setShowQueueControl(false)
                                }
                            }}
                        >
                            <div
                                className="control-container"
                                style={{
                                    width: "100%",
                                    maxWidth: "520px",
                                    maxHeight: "90vh",
                                    overflowY: "auto",
                                    background: "#fff",
                                    borderRadius: "12px",
                                    padding: "24px",
                                    boxShadow: "0 8px 32px rgba(0,0,0,0.18)",
                                }}
                            >
                                <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", flexWrap: "wrap", marginBottom: "16px" }}>
                                    <h2 style={{ marginBottom: 0 }}>Queue Control — {queueControlChannel}</h2>
                                    <button
                                        type="button"
                                        className="btn btn-secondary"
                                        onClick={() => {
                                            setQueueControlChannel(null)
                                            setShowQueueControl(false)
                                        }}
                                    >
                                        Close
                                    </button>
                                </div>
                                <div style={{ margin: "0 0 16px" }}>
                                    <button className="btn btn-primary" onClick={fetchQueueStats}>Refresh Queue Status</button>
                                </div>

                                <div
                                    style={{
                                        border: "1px solid #dbe2ea",
                                        borderRadius: "10px",
                                        padding: "16px",
                                        marginBottom: "18px",
                                        background: "#f8fafc"
                                    }}
                                >
                                    <h4 style={{ marginTop: 0, marginBottom: "12px" }}>Failover Status</h4>
                                    <div style={{
                                        display: "grid",
                                        gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
                                        gap: "12px"
                                    }}>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Primary Queue</div>
                                            <div style={{ fontWeight: 600, wordBreak: "break-word" }}>
                                                {selectedQueueStats?.primary_queue || `${queueControlChannel.toLowerCase()}-notify-q`}
                                            </div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Active Queue</div>
                                            <div style={{ fontWeight: 600, wordBreak: "break-word" }}>
                                                {selectedQueueStats?.active_queue || "-"}
                                            </div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Dynamic Backup Queue</div>
                                            <div style={{ fontWeight: 600, wordBreak: "break-word" }}>
                                                {selectedQueueStats?.backup_queue || "Not created"}
                                            </div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Current State</div>
                                            <div style={{ fontWeight: 600 }}>
                                                {renderFailoverReasons(selectedQueueStats?.failover_reasons)}
                                            </div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Primary Waiting</div>
                                            <div style={{ fontWeight: 600 }}>{selectedQueueStats?.waiting ?? 0}</div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Backup Waiting</div>
                                            <div style={{ fontWeight: 600 }}>{selectedQueueStats?.backup_waiting ?? 0}</div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Last Failover Reason</div>
                                            <div style={{ fontWeight: 600 }}>
                                                {selectedQueueStats?.last_failover_reason || "None"}
                                            </div>
                                        </div>
                                        <div>
                                            <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>Last Failover Time</div>
                                            <div style={{ fontWeight: 600 }}>
                                                {formatDateTime(selectedQueueStats?.last_failover_at)}
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <div className="control-buttons">
                                    <button className="btn btn-warning" onClick={pauseQueue}>Pause</button>
                                    <button className="btn btn-success" onClick={resumeQueue}>Resume</button>
                                    <button className="btn btn-danger" onClick={clearQueue}>Clear Queue</button>
                                </div>

                                <div className="control-buttons">
                                    <button className="btn btn-primary" onClick={retryFailed}>Retry Failed</button>
                                    <button className="btn btn-secondary" onClick={reprocessDLQ}>Reprocess DLQ</button>
                                </div>

                                <div className="rate-limit-box">
                                    <label>Rate Limit (jobs/min)</label>
                                    <div className="rate-input-group">
                                        <input
                                            type="number"
                                            value={rateLimitDraft}
                                            onChange={(e) => {
                                                setRateLimitDraft(e.target.value)
                                                setRateLimitTouched(true)
                                            }}
                                            className="form-control"
                                        />
                                        <button className="btn btn-update" onClick={updateRateLimit}>Update</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}
                </>
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
                            <option value="WHATSAPP">WHATSAPP</option>
                            <option value="PUSH">PUSH</option>
                        </select>
                        <button
                            type="button"
                            onClick={discardAllDlq}
                            style={{
                                marginLeft: "8px",
                                background: "#dc2626",
                                color: "#fff",
                                border: "none",
                                borderRadius: "6px",
                                padding: "8px 12px",
                                fontWeight: 600,
                                cursor: "pointer",
                            }}
                        >
                            Discard All
                        </button>
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
                                                ? formatUtcTimestamp(job.failed_at)
                                                : "-"}
                                        </td>
                                        <td>
                                            <button
                                                onClick={() => retryJob(job.job_id)}
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
                                onClick={() => copyToClipboard(clientId, "tokens-client-id")}
                                style={{ background: "#334155", color: "#fff" }}
                            >
                                {copiedTarget === "tokens-client-id" ? "✓" : "Copy"}
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
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>

                        <tbody>
                            {tokens.map((t, i) => (
                                <tr key={i}>
                                    <td>{t.event_type}</td>
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
            
            {/* -------- JOBS + ACTIVITY (MERGED) -----------*/ }
            {activeTab === "activity" && (() => {
                // ------ Start from jobs, then enrich with logs ------
                // /jobs returns server-side per-recipient outcome counts, which avoids
                // undercounting when /logs is paginated during large load tests.
                const collapsed = {}
                for (const job of jobs) {
                    const jid = job.job_id || "__no_id__"
                    const succeededUserIds = Array.isArray(job.succeeded_user_ids) ? job.succeeded_user_ids : []
                    const failedUserIds = Array.isArray(job.failed_user_ids) ? job.failed_user_ids : []
                    const userIds = new Set(Array.isArray(job.user_ids) ? job.user_ids : [])
                    for (const u of succeededUserIds) userIds.add(u)
                    for (const u of failedUserIds) userIds.add(u)

                    collapsed[jid] = {
                        job_id:             jid,
                        client_id:          job.client_id || "-",
                        event_type:         job.event_type || "-",
                        channel:            job.channel || "-",
                        user_ids:           userIds,
                        recipientOutcomes:  {},
                        succeeded_user_ids: succeededUserIds,
                        failed_user_ids:    failedUserIds,
                        job_created_at:     job.created_at || null,
                        log_status:         job.latest_log_status || job.status || "-",
                        log_time:           job.latest_log_time || job.updated_at || null,
                        error:              job.latest_error || null,
                        _latestMs:          new Date(job.latest_log_time || job.updated_at || job.created_at || 0).getTime(),
                    }
                }

                for (const log of logs) {
                    const jid = log.job_id || "__no_id__"
                    const job = collapsed[jid] || log.job || {}

                    if (!collapsed[jid]) {
                        collapsed[jid] = {
                            job_id:          jid,
                            client_id:       job.client_id  || log.client_id  || "-",
                            event_type:      job.event_type || log.event_type || "-",
                            channel:         job.channel    || log.channel    || "-",
                            user_ids:        new Set(Array.isArray(job.user_ids) ? job.user_ids : []),
                            recipientOutcomes: {},
                            succeeded_user_ids: [],
                            failed_user_ids: [],
                            job_created_at:  job.created_at || null,
                            // latest-log fields (updated below if a newer log comes)
                            log_status:      log.status || "-",
                            log_time:        log.created_at || null,
                            error:           log.error || null,
                            _latestMs:       new Date(log.created_at || 0).getTime(),
                        }
                    } else {
                        const ms = new Date(log.created_at || 0).getTime()
                        if (ms > collapsed[jid]._latestMs) {
                            collapsed[jid].log_status = log.status || "-"
                            collapsed[jid].log_time   = log.created_at || null
                            collapsed[jid].error      = log.error || null
                            collapsed[jid]._latestMs  = ms
                        }
                    }

                    if (Array.isArray(job.user_ids)) {
                        for (const u of job.user_ids) collapsed[jid].user_ids.add(u)
                    }

                    const recipientUserId = log.recipient_user_id || log.job?.recipient_user_id
                    if (recipientUserId) {
                        collapsed[jid].user_ids.add(recipientUserId)
                        const priorOutcome = collapsed[jid].recipientOutcomes[recipientUserId]
                        const currentMs = new Date(log.created_at || 0).getTime()
                        if (!priorOutcome || currentMs >= priorOutcome.ms) {
                            collapsed[jid].recipientOutcomes[recipientUserId] = {
                                status: String(log.status || "").toUpperCase(),
                                ms: currentMs,
                            }
                        }
                    }
                }

                // Convert map → array, resolve Set → array, sort newest job first
                const merged = Object.values(collapsed).map((row) => ({
                    ...row,
                    user_ids: [...row.user_ids],
                    succeeded_user_ids: row.succeeded_user_ids.length
                        ? row.succeeded_user_ids
                        : Object.entries(row.recipientOutcomes)
                        .filter(([, outcome]) => SUCCESS_ACTIVITY_STATUSES.has(outcome.status))
                        .map(([userId]) => userId),
                    failed_user_ids: row.failed_user_ids.length
                        ? row.failed_user_ids
                        : Object.entries(row.recipientOutcomes)
                        .filter(([, outcome]) => FAILED_ACTIVITY_STATUSES.has(outcome.status))
                        .map(([userId]) => userId),
                })).sort((a, b) =>
                    new Date(b.job_created_at || 0).getTime() - new Date(a.job_created_at || 0).getTime()
                )

                const filtered = merged.filter((row) => {
                    const jidOk = !filterJobId     || row.job_id.toLowerCase().includes(filterJobId.toLowerCase())
                    const chOk  = !filterChannel2  || row.channel.toLowerCase() === filterChannel2.toLowerCase()
                    const evOk  = !filterEventType2|| row.event_type.toLowerCase().includes(filterEventType2.toLowerCase())
                    const cidOk = !filterClientId2 || row.client_id.toLowerCase().includes(filterClientId2.toLowerCase())
                    const stOk  = !filterStatus2   || row.log_status.toUpperCase() === filterStatus2.toUpperCase()
                    return jidOk && chOk && evOk && cidOk && stOk
                })

                return (
                    <>
                        <h2>Jobs + Activity</h2>

                        <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap" }}>
                            <input
                                type="text"
                                placeholder="Filter by job_id"
                                value={filterJobId}
                                onChange={(e) => setFilterJobId(e.target.value)}
                                style={{ width: "200px", maxWidth: "100%" }}
                            />
                            <select value={filterChannel2} onChange={(e) => setFilterChannel2(e.target.value)}>
                                <option value="">All Channels</option>
                                <option value="email">email</option>
                                <option value="sms">sms</option>
                                <option value="whatsapp">whatsapp</option>
                                <option value="push">push</option>
                            </select>
                            <input
                                type="text"
                                placeholder="Filter by event_type"
                                value={filterEventType2}
                                onChange={(e) => setFilterEventType2(e.target.value)}
                                style={{ width: "200px", maxWidth: "100%" }}
                            />
                            <input
                                type="text"
                                placeholder="Filter by client_id"
                                value={filterClientId2}
                                onChange={(e) => setFilterClientId2(e.target.value)}
                                style={{ width: "200px", maxWidth: "100%" }}
                            />
                            <select value={filterStatus2} onChange={(e) => setFilterStatus2(e.target.value)}>
                                <option value="">All Statuses</option>
                                <option value="QUEUED">QUEUED</option>
                                <option value="PROCESSING">PROCESSING</option>
                                <option value="SENT">SENT</option>
                                <option value="DELIVERED">DELIVERED</option>
                                <option value="READ">READ</option>
                                <option value="DLQ">DLQ</option>
                                <option value="FAILED">FAILED</option>
                                <option value="DROPPED">DROPPED</option>
                            </select>
                        </div>

                        <table>
                            <thead>
                                <tr>
                                    <th>Job ID</th>
                                    <th>Client ID</th>
                                    <th>Event</th>
                                    <th>Channel</th>
                                    <th>Status</th>
                                    <th>Succeeded Tasks</th>
                                    <th>Failed Tasks</th>
                                    <th>Job Creation Time</th>
                                    <th>Log Receiving Time</th>
                                    <th>Error</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filtered.map((row) => (
                                    <tr key={row.job_id}>
                                        <td>
                                            {renderActivityLink(row.job_id, () =>
                                                openActivityDetail(
                                                    `All attempted user IDs for ${row.job_id}`,
                                                    row.user_ids,
                                                    "No user IDs were found for this job."
                                                )
                                            )}
                                        </td>
                                        <td>{row.client_id}</td>
                                        <td>{row.event_type}</td>
                                        <td>{row.channel}</td>
                                        <td>{row.log_status}</td>
                                        <td>
                                            {renderActivityLink(String(row.succeeded_user_ids.length), () =>
                                                openActivityDetail(
                                                    `Succeeded user IDs for ${row.job_id}`,
                                                    row.succeeded_user_ids,
                                                    "No successful recipients were found for this job."
                                                )
                                            )}
                                        </td>
                                        <td>
                                            {renderActivityLink(String(row.failed_user_ids.length), () =>
                                                openActivityDetail(
                                                    `Failed user IDs for ${row.job_id}`,
                                                    row.failed_user_ids,
                                                    "No failed recipients were found for this job."
                                                )
                                            )}
                                        </td>
                                        <td>{row.job_created_at ? formatAdminTimestamp(row.job_created_at, "Z") : "-"}</td>
                                        <td>{row.log_time ? formatUtcTimestamp(row.log_time) : "-"}</td>                                        <td>{toDisplayText(row.error)}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </>
                )
            })()}

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
                                <th>Delivered At</th>
                                <th>Read At</th>
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
                                    <td>{w.created_at ? formatAdminTimestamp(w.created_at, "+05:30") : "-"}</td>
                                    <td>{w.delivered_at ? formatAdminTimestamp(w.delivered_at, "+05:30") : "-"}</td>
                                    <td>{w.read_at ? formatAdminTimestamp(w.read_at, "+05:30") : "-"}</td>
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
                <div style={{ display: "flex", gap: "8px", marginBottom: "12px", flexWrap: "wrap", alignItems: "center" }}>
                    <input
                        type="text"
                        placeholder="Search by client id or name"
                        value={clientSearch}
                        onChange={(e) => setClientSearch(e.target.value)}
                        style={{ width: "280px", maxWidth: "100%" }}
                    />
                    <select value={clientStatusFilter} onChange={(e) => setClientStatusFilter(e.target.value)}>
                        <option value="all">All</option>
                        <option value="active">Active</option>
                        <option value="inactive">Inactive</option>
                    </select>

                    {/* Select button — toggles checkbox mode */}
                    <button
                        onClick={() => {
                            setClientSelectMode(m => !m)
                            setSelectedClients(new Set())
                        }}
                        style={{
                            background: clientSelectMode ? "#1d4ed8" : "#e2e8f0",
                            color: clientSelectMode ? "#fff" : "#374151",
                            border: "none", borderRadius: "6px",
                            padding: "8px 14px", fontWeight: 600, cursor: "pointer"
                        }}
                    >
                        {clientSelectMode ? "Cancel Select" : "Select"}
                    </button>

                    {/* Bulk action bar — only shown when in select mode and something is selected */}
                    {clientSelectMode && selectedClients.size > 0 && (
                        <div style={{ display: "flex", gap: "8px", alignItems: "center", background: "#fef3c7", border: "1px solid #f59e0b", borderRadius: "6px", padding: "6px 12px" }}>
                            <span style={{ fontSize: "13px", fontWeight: 600, color: "#92400e" }}>
                                {selectedClients.size} selected
                            </span>
                            <button
                                onClick={deleteSelectedClients}
                                style={{ background: "#dc2626", color: "#fff", border: "none", borderRadius: "6px", padding: "6px 12px", fontWeight: 600, cursor: "pointer", fontSize: "13px" }}
                            >
                                Delete Selected
                            </button>
                            <button
                                onClick={() => setSelectedClients(new Set())}
                                style={{ background: "transparent", color: "#92400e", border: "1px solid #f59e0b", borderRadius: "6px", padding: "6px 10px", cursor: "pointer", fontSize: "13px" }}
                            >
                                Clear
                            </button>
                        </div>
                    )}

                    <button
                        onClick={deleteAllClientAccounts}
                        style={{ background: "#991b1b", color: "#fff", border: "none", borderRadius: "6px", padding: "8px 12px", fontWeight: 700, cursor: "pointer" }}
                    >
                        Delete All Accounts
                    </button>
                </div>
                <table>
                    <thead>
                        <tr>
                            {clientSelectMode && (
                                <th>
                                    <input
                                        type="checkbox"
                                        title="Select all visible"
                                        onChange={(e) => {
                                            const visible = clients.filter((c) => {
                                                const byStatus = clientStatusFilter === "all" || (clientStatusFilter === "active" && c.is_active) || (clientStatusFilter === "inactive" && !c.is_active)
                                                const q = clientSearch.trim().toLowerCase()
                                                const bySearch = !q || String(c.client_id || "").toLowerCase().includes(q) || String(c.name || "").toLowerCase().includes(q)
                                                return byStatus && bySearch
                                            })
                                            if (e.target.checked) setSelectedClients(new Set(visible.map(c => c.client_id)))
                                            else setSelectedClients(new Set())
                                        }}
                                        checked={(() => {
                                            const visible = clients.filter((c) => {
                                                const byStatus = clientStatusFilter === "all" || (clientStatusFilter === "active" && c.is_active) || (clientStatusFilter === "inactive" && !c.is_active)
                                                const q = clientSearch.trim().toLowerCase()
                                                const bySearch = !q || String(c.client_id || "").toLowerCase().includes(q) || String(c.name || "").toLowerCase().includes(q)
                                                return byStatus && bySearch
                                            })
                                            return visible.length > 0 && visible.every(c => selectedClients.has(c.client_id))
                                        })()}
                                    />
                                </th>
                            )}
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
                                const byStatus = clientStatusFilter === "all" || (clientStatusFilter === "active" && c.is_active) || (clientStatusFilter === "inactive" && !c.is_active)
                                const q = clientSearch.trim().toLowerCase()
                                const bySearch = !q || String(c.client_id || "").toLowerCase().includes(q) || String(c.name || "").toLowerCase().includes(q)
                                return byStatus && bySearch
                            })
                            .map((c) => (
                                <tr key={c.client_id} style={{ background: selectedClients.has(c.client_id) ? "#eff6ff" : undefined }}>
                                    {clientSelectMode && (
                                        <td>
                                            <input
                                                type="checkbox"
                                                checked={selectedClients.has(c.client_id)}
                                                onChange={(e) => {
                                                    const next = new Set(selectedClients)
                                                    if (e.target.checked) next.add(c.client_id)
                                                    else next.delete(c.client_id)
                                                    setSelectedClients(next)
                                                }}
                                            />
                                        </td>
                                    )}
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
                                                onChange={(e) => setQuotaDrafts((prev) => ({ ...prev, [c.client_id]: e.target.value }))}
                                                style={{ width: "110px" }}
                                            />
                                            <button onClick={() => updateClientQuota(c)}>Save</button>
                                        </div>
                                    </td>
                                    <td>{(c.allowed_channels || []).join(", ") || "-"}</td>
                                    <td>{c.created_at ? new Date(c.created_at).toLocaleString() : "-"}</td>
                                    <td>
                                        <div style={{ display: "flex", gap: "6px", flexWrap: "wrap", alignItems: "center" }}>
                                            <button onClick={() => toggleClientActive(c)}>
                                                {c.is_active ? "Deactivate" : "Activate"}
                                            </button>
                                            <button onClick={() => rotateClientKey(c)}>Rotate Key</button>
                                            <button
                                                onClick={() => deleteClientAccount(c)}
                                                style={{ background: "#dc2626", color: "#fff", border: "none", borderRadius: "6px", padding: "8px 12px", fontWeight: 600, cursor: "pointer" }}
                                            >
                                                Delete Account
                                            </button>
                                        </div>
                                    </td>
                                </tr>
                            ))}
                    </tbody>
                </table>
            </>
)}

            {activeTab === "providers" && (
                <><ProviderSettings /></>
            )}

        </div>
        {activityDetail && (
            <div
                style={{
                    position: "fixed",
                    inset: 0,
                    background: "rgba(2, 6, 23, 0.72)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    zIndex: 1100,
                    padding: "16px",
                }}
            >
                <div
                    style={{
                        width: "100%",
                        maxWidth: "720px",
                        maxHeight: "80vh",
                        overflowY: "auto",
                        background: "#ffffff",
                        border: "1px solid #dbe2ea",
                        borderRadius: "10px",
                        padding: "22px",
                        boxShadow: "0 12px 36px rgba(15, 23, 42, 0.22)",
                    }}
                >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
                        <h3 style={{ margin: 0 }}>{activityDetail.title}</h3>
                        <button
                            type="button"
                            className="btn btn-secondary"
                            onClick={() => setActivityDetail(null)}
                        >
                            Close
                        </button>
                    </div>

                    {activityDetail.userIds.length ? (
                        <div
                            style={{
                                display: "grid",
                                gap: "8px",
                                gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                            }}
                        >
                            {activityDetail.userIds.map((userId) => (
                                <div
                                    key={userId}
                                    style={{
                                        border: "1px solid #e2e8f0",
                                        borderRadius: "8px",
                                        padding: "10px 12px",
                                        background: "#f8fafc",
                                        wordBreak: "break-word",
                                    }}
                                >
                                    {userId}
                                </div>
                            ))}
                        </div>
                    ) : (
                        <div style={{ color: "#64748b" }}>{activityDetail.emptyMessage}</div>
                    )}
                </div>
            </div>
        )}

        {selectedJob && (
            <div
                style={{
                    position: "fixed",
                    inset: 0,
                    background: "rgba(2, 6, 23, 0.72)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    zIndex: 1000,
                    padding: "16px",
                }}
                onClick={(e) => {
                    if (e.target === e.currentTarget) {
                        setSelectedJob(null)
                    }
                }}
            >
                <div
                    style={{
                        width: "100%",
                        maxWidth: "760px",
                        maxHeight: "88vh",
                        overflowY: "auto",
                        background: "#ffffff",
                        border: "1px solid #dbe2ea",
                        borderRadius: "10px",
                        padding: "22px",
                        boxShadow: "0 12px 36px rgba(15, 23, 42, 0.22)",
                    }}
                >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", marginBottom: "14px" }}>
                        <h3 style={{ margin: 0 }}>DLQ Job Details</h3>
                        <button
                            type="button"
                            className="btn btn-secondary"
                            onClick={() => setSelectedJob(null)}
                        >
                            Close
                        </button>
                    </div>

                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: "12px", marginBottom: "16px" }}>
                        <div>
                            <div style={{ fontSize: "12px", color: "#64748b" }}>Job ID</div>
                            <div style={{ fontWeight: 600, wordBreak: "break-word" }}>{selectedJob.job_id || "-"}</div>
                        </div>
                        <div>
                            <div style={{ fontSize: "12px", color: "#64748b" }}>Queue</div>
                            <div style={{ fontWeight: 600 }}>{selectedJob.queue_name || selectedJob.channel || "-"}</div>
                        </div>
                        <div>
                            <div style={{ fontSize: "12px", color: "#64748b" }}>Status</div>
                            <div style={{ fontWeight: 600 }}>{selectedJob.status || "-"}</div>
                        </div>
                        <div>
                            <div style={{ fontSize: "12px", color: "#64748b" }}>Failed At</div>
                            <div style={{ fontWeight: 600 }}>{formatUtcTimestamp(selectedJob.failed_at)}</div>
                        </div>
                        <div>
                            <div style={{ fontSize: "12px", color: "#64748b" }}>Error Type</div>
                            <div style={{ fontWeight: 600 }}>{selectedJob.error_type || "-"}</div>
                        </div>
                        <div>
                            <div style={{ fontSize: "12px", color: "#64748b" }}>Error Code</div>
                            <div style={{ fontWeight: 600 }}>{selectedJob.error_code || "-"}</div>
                        </div>
                    </div>

                    <div style={{ marginBottom: "16px" }}>
                        <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "6px" }}>Error Message</div>
                        <div style={{ background: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: "8px", padding: "10px", whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
                            {toDisplayText(selectedJob.error_message)}
                        </div>
                    </div>

                    <div style={{ marginBottom: "16px" }}>
                        <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "6px" }}>Payload</div>
                        <pre style={{ background: "#0f172a", color: "#e2e8f0", borderRadius: "8px", padding: "12px", overflowX: "auto", fontSize: "12px" }}>
                            {JSON.stringify(selectedJob.payload || selectedJob.content || {}, null, 2)}
                        </pre>
                    </div>

                    <div style={{ display: "flex", justifyContent: "flex-end", gap: "8px", flexWrap: "wrap" }}>
                        <button
                            type="button"
                            className="btn btn-primary"
                            onClick={async () => {
                                if (await retryJob(selectedJob.job_id)) {
                                    setSelectedJob(null)
                                }
                            }}
                        >
                            Retry
                        </button>
                        <button
                            type="button"
                            className="btn btn-danger"
                            onClick={async () => {
                                if (await discardJob(selectedJob.job_id)) {
                                    setSelectedJob(null)
                                }
                            }}
                        >
                            Discard
                        </button>
                    </div>
                </div>
            </div>
        )}

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
                            onClick={() => copyToClipboard(tokenModal.apiKey, "token-modal-key")}
                            style={{ background: "#2563eb", color: "#fff" }}
                        >
                            {copiedTarget === "token-modal-key" ? "✓" : "Copy Key"}
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
