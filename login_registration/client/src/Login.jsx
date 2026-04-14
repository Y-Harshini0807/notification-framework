import { useState } from "react"
import axios from "axios"
import { Link, useNavigate } from "react-router-dom"
import "./Auth.css"

function Login() {
    const [email, setEmail] = useState("")
    const [password, setPassword] = useState("")
    const [errorMsg, setErrorMsg] = useState("")
    const [successMsg, setSuccessMsg] = useState("")
    const [loading, setLoading] = useState(false)

    const navigate = useNavigate()

    const handleSubmit = async (e) => {
        e.preventDefault()
        
        // 1. Check if email and password are provided
        if (!email.trim() || !password.trim()) {
            setErrorMsg("Please enter both email and password.")
            return
        }

        setErrorMsg("")
        setSuccessMsg("")
        setLoading(true)

        try {
            const res = await axios.post("http://localhost:3001/login", {
                email,
                password
            })
            console.log("TOKEN:", res.data.token) // DEBUG
            localStorage.setItem("token", res.data.token)

            setSuccessMsg("Logged in successfully")
            navigate("/home")

        } catch (err) {
            setErrorMsg(err.response?.data?.message || "Invalid email or password")
        } finally {
            // 2. Stop the loading state whether it succeeds or fails
            setLoading(false)
        }
    }

    return (
        <div className="auth-container">
            <div className="auth-card">
                <h2>Login</h2>

                {errorMsg && <p className="error-text">{errorMsg}</p>}
                {successMsg && <p className="success-text">{successMsg}</p>}

                <form onSubmit={handleSubmit}>
                    {/* 3. Added 'required' attribute */}
                    <input type="email" placeholder="Email"
                        className="form-control"
                        value={email}
                        onChange={e => setEmail(e.target.value)} 
                        required 
                    />

                    {/* 3. Added 'required' attribute */}
                    <input type="password" placeholder="Password"
                        className="form-control"
                        value={password}
                        onChange={e => setPassword(e.target.value)} 
                        required 
                    />

                    <button className="btn btn-register w-100" disabled={loading}>
                        {loading ? "Logging in..." : "Login"}
                    </button>
                </form>

                {/* Updated styling to match the new Auth.css layout */}
                <Link to="/register" className="btn btn-login w-100 mt-2" style={{ display: 'block', textAlign: 'center', textDecoration: 'none', lineHeight: '46px' }}>
                    Register
                </Link>
            </div>
        </div>
    )
}

export default Login