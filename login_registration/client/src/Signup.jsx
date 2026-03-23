import { useState } from "react"
import axios from "axios"
import { Link, useNavigate } from "react-router-dom"
import "./Auth.css"

function Signup() {
    const [name, setName] = useState("")
    const [client_name, setClient_name] = useState("")
    const [email, setEmail] = useState("")
    const [password, setPassword] = useState("")
    const [errorMsg, setErrorMsg] = useState("")
    const [successMsg, setSuccessMsg] = useState("")
    const [apiKey, setApiKey] = useState("")
    const [loading, setLoading] = useState(false)
    const navigate = useNavigate()

    const handleSubmit = async (e) => {
        e.preventDefault()

        // 1. Check if all fields have a value before doing anything
        if (!name.trim() || !client_name.trim() || !email.trim() || !password.trim()) {
            setErrorMsg("Please fill in all the fields.")
            return
        }

        // Clear any previous errors and start loading
        setErrorMsg("")
        setLoading(true)

        try {
            const res = await axios.post("http://localhost:3001/register", {
                name,
                client_name,
                email,
                password
            })

            // ✅ CASE: NEW USER
            if (res.data.status === "created") {
                localStorage.setItem("token", res.data.token)
                alert(`Save your API Key: ${res.data.api_key}`)
                navigate("/home")   // 🔥 direct to home
            }

        } catch (err) {
            // ✅ CASE: USER EXISTS
            if (err.response?.data?.status === "exists") {
                alert("Account already exists. Please login.")
                navigate("/login")
            } else {
                setErrorMsg("Server error. Please try again.")
            }
        } finally {
            // Stop the loading spinner whether it succeeded or failed
            setLoading(false)
        }
    }

    return (
        <div className="auth-container">
            <div className="auth-card">
                <h2>Create Account</h2>

                {errorMsg && <p className="error-text">{errorMsg}</p>}
                {successMsg && <p className="success-text">{successMsg}</p>}

                {apiKey && (
                    <div className="api-key-box">
                        <p><strong>⚠️ Save this API Key (shown only once)</strong></p>
                        <textarea value={apiKey} readOnly />
                    </div>
                )}

                <form onSubmit={handleSubmit}>
                    {/* Added 'required' to all inputs */}
                    <input type="text" placeholder="Name" className="form-control"
                        value={name} onChange={e => setName(e.target.value)} required />

                    {/* Removed (optional) from placeholder */}
                    <input type="text" placeholder="Organization" 
                        className="form-control"
                        value={client_name} onChange={e => setClient_name(e.target.value)} required />

                    <input type="email" placeholder="Email"
                        className="form-control"
                        value={email} onChange={e => setEmail(e.target.value)} required />

                    <input type="password" placeholder="Password"
                        className="form-control"
                        value={password} onChange={e => setPassword(e.target.value)} required />

                    <button className="btn btn-register w-100" disabled={loading}>
                        {loading ? "Creating..." : "Register"}
                    </button>
                </form>

                <Link to="/login" className="btn btn-login w-100 mt-2" style={{ display: 'block', textAlign: 'center', textDecoration: 'none', lineHeight: '46px' }}>
                    Login
                </Link>
            </div>
        </div>
    )
}

export default Signup