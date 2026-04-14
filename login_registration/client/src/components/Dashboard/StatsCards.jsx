import React from "react";

const StatsCards = ({ stats }) => {
    return (
        <div style={{ display: "flex", gap: "20px", marginBottom: "20px" }}>
            <div style={cardStyle}>
                <h3>Waiting</h3>
                <p>{stats.waiting}</p>
            </div>

            <div style={cardStyle}>
                <h3>Active</h3>
                <p>{stats.active}</p>
            </div>

            <div style={cardStyle}>
                <h3>Failed</h3>
                <p>{stats.failed}</p>
            </div>
        </div>
    );
};

const cardStyle = {
    padding: "20px",
    border: "1px solid #ccc",
    borderRadius: "10px",
    width: "120px",
    textAlign: "center"
};

export default StatsCards;