import React from "react"

class ErrorBoundary extends React.Component {
    constructor(props) {
        super(props)
        this.state = {
            hasError: false,
            error: null,
            errorInfo: null,
        }
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error }
    }

    componentDidCatch(error, errorInfo) {
        this.setState({ errorInfo })
        console.error("Runtime UI error:", error, errorInfo)
    }

    handleReload = () => {
        window.location.reload()
    }

    render() {
        if (this.state.hasError) {
            return (
                <div style={{
                    minHeight: "100vh",
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                    background: "#f8fafc",
                    padding: "24px",
                    boxSizing: "border-box",
                }}>
                    <div style={{
                        width: "100%",
                        maxWidth: "900px",
                        background: "#ffffff",
                        border: "1px solid #e2e8f0",
                        borderRadius: "12px",
                        boxShadow: "0 8px 30px rgba(15, 23, 42, 0.08)",
                        padding: "24px",
                    }}>
                        <h2 style={{ marginTop: 0, color: "#0f172a" }}>Something went wrong in the UI</h2>
                        <p style={{ color: "#475569" }}>
                            The page crashed at runtime. Error details are shown below.
                        </p>
                        <button
                            onClick={this.handleReload}
                            style={{
                                background: "#2563eb",
                                color: "#fff",
                                border: "none",
                                borderRadius: "8px",
                                padding: "8px 14px",
                                cursor: "pointer",
                                marginBottom: "16px",
                            }}
                        >
                            Reload page
                        </button>
                        <pre style={{
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                            background: "#0f172a",
                            color: "#e2e8f0",
                            borderRadius: "8px",
                            padding: "12px",
                            marginBottom: "12px",
                            fontSize: "13px",
                        }}>
                            {String(this.state.error?.stack || this.state.error || "Unknown runtime error")}
                        </pre>
                        {this.state.errorInfo?.componentStack && (
                            <pre style={{
                                whiteSpace: "pre-wrap",
                                wordBreak: "break-word",
                                background: "#1e293b",
                                color: "#cbd5e1",
                                borderRadius: "8px",
                                padding: "12px",
                                margin: 0,
                                fontSize: "12px",
                            }}>
                                {this.state.errorInfo.componentStack}
                            </pre>
                        )}
                    </div>
                </div>
            )
        }

        return this.props.children
    }
}

export default ErrorBoundary
