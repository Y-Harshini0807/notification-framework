export const queueStats = {
    waiting: 12,
    active: 5,
    failed: 3
};

export const failedJobs = [
    { id: "job1", error: "Timeout error" },
    { id: "job2", error: "Webhook failed" }
];

export const clientData = {
    client_id: "abc123",
    is_active: true,
    tokens: [
        { event_type: "PAYMENT", is_active: true },
        { event_type: "ALERT", is_active: false }
    ]
};