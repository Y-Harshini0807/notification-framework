import React from "react";

const QueueControls = () => {

    const handlePause = () => {
        alert("Queue Paused (mock)");
    };

    const handleResume = () => {
        alert("Queue Resumed (mock)");
    };

    const handleKill = () => {
        alert("Queue Killed (mock)");
    };

    return (
        <div style={{ marginBottom: "20px" }}>
            <h3>Queue Controls</h3>
            <button onClick={handlePause}>Pause</button>
            <button onClick={handleResume}>Resume</button>
            <button onClick={handleKill}>Kill</button>
        </div>
    );
};

export default QueueControls;