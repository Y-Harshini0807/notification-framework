import React, { useState } from "react";

const ClientPanel = ({ client }) => {
    const [isActive, setIsActive] = useState(client.is_active);

    const toggleStatus = () => {
        setIsActive(!isActive);
    };

    return (
        <div>
            <h3>Client Panel</h3>
            <p><strong>Client ID:</strong> {client.client_id}</p>

            <p>
                <strong>Status:</strong> {isActive ? "Active" : "Disabled"}
            </p>

            <button onClick={toggleStatus}>
                {isActive ? "Disable" : "Enable"}
            </button>

            <h4>Event Tokens</h4>
            <ul>
                {client.tokens.map((t, index) => (
                    <li key={index}>
                        {t.event_type} - {t.is_active ? "Active" : "Inactive"}
                    </li>
                ))}
            </ul>
        </div>
    );
};

export default ClientPanel;