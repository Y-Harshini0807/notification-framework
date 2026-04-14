import React from "react";
import StatsCards from "../components/Dashboard/StatsCards";
import QueueControls from "../components/Dashboard/QueueControls";
import DLQTable from "../components/Dashboard/DLQTable";
import ClientPanel from "../components/Dashboard/ClientPanel";

import { queueStats, failedJobs, clientData } from "../mock/mockData";

const Dashboard = () => {
    return (
        <div style={{ padding: "20px" }}>
            <h2>Admin Dashboard</h2>

            <StatsCards stats={queueStats} />
            <QueueControls />
            <DLQTable jobs={failedJobs} />
            <ClientPanel client={clientData} />
        </div>
    );
};

export default Dashboard;