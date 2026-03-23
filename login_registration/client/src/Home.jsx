import { useState, useEffect } from "react"
import axios from "axios"
import { useNavigate } from "react-router-dom"
import "./Auth.css"

function Home() {
    const [clientId, setClientId] = useState("")
    const [tokens, setTokens] = useState([])
    const [newEvent, setNewEvent] = useState("")
    const [newApiKey, setNewApiKey] = useState("")
    const [loading, setLoading] = useState(true)

    const navigate = useNavigate()

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

            setClientId(res.data.client_id)
            setTokens(res.data.event_tokens)

        } catch (err) {
            console.log("Auth error:", err.response?.data || err.message)

            // 🔥 IMPORTANT FIX
            localStorage.removeItem("token")
            // navigate("/login")
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        const token = localStorage.getItem("token")

        // 🔥 CRITICAL FIX: check token before API call
        if (!token) {
            navigate("/login")
            return
        }

        fetchData()
    }, [])

    const handleLogout = () => {
        localStorage.removeItem("token")
        navigate("/login")
    }

    const createToken = async () => {
        if (!newEvent.trim()) return alert("Enter event type")

        try {
            const res = await axios.post(
                "http://localhost:3001/create-token",
                { event_type: newEvent },
                getAuthHeader()
            )

            setNewApiKey(res.data.api_key)
            setNewEvent("")
            fetchData()

        } catch (err) {
            console.log("Create token error:", err.response?.data || err.message)
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

            alert(`New API Key: ${res.data.api_key}`)

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
            alert("Failed to disable token")
        }
    }

    // 🔥 Optional: loading state (prevents flicker)
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
        <div className="auth-container">
            <div className="auth-card">
                <h2>Dashboard</h2>

                <h4>Create API Key</h4>
                <input
                    placeholder="Event Type"
                    value={newEvent}
                    onChange={e => setNewEvent(e.target.value)}
                    className="form-control"
                />

                <button onClick={createToken} className="btn btn-register mt-2">
                    Generate Token
                </button>

                {newApiKey && (
                    <div className="api-key-box">
                        <p><strong>⚠️ Save this API Key (shown once)</strong></p>
                        <textarea value={newApiKey} readOnly />
                    </div>
                )}

                <h4 className="mt-3">Your Tokens</h4>

                {tokens.length === 0 && <p>No tokens created yet</p>}

                {tokens.map((t, i) => (
                    <div key={i} className="token-card">
                        <p><strong>{t.event_type}</strong></p>
                        <p>Status: {t.is_active ? "Active" : "Disabled"}</p>

                        <div className="token-actions">
                            <button onClick={() => refreshToken(t.event_type)} className="refresh-btn">
                                Refresh
                            </button>
                            <button onClick={() => disableToken(t.event_type)} className="disable-btn">
                                Disable
                            </button>
                        </div>
                    </div>
                ))}

                <button onClick={handleLogout} className="logout-btn mt-3">
                    Logout
                </button>
            </div>
        </div>
    )
}

export default Home