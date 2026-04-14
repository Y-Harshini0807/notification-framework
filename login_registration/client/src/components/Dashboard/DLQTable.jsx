import React from "react";

const DLQTable = ({ jobs }) => {

    const handleRetry = (id) => {
        alert(`Retry job ${id} (mock)`);
    };

    return (
        <div style={{ marginBottom: "20px" }}>
            <h3>Dead Letter Queue</h3>
            <table border="1" cellPadding="10">
                <thead>
                    <tr>
                        <th>Job ID</th>
                        <th>Error</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
                    {jobs.map((job) => (
                        <tr key={job.id}>
                            <td>{job.id}</td>
                            <td>{job.error}</td>
                            <td>
                                <button onClick={() => handleRetry(job.id)}>
                                    Retry
                                </button>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

export default DLQTable;