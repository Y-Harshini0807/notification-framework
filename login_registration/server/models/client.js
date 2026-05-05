const mongoose = require('mongoose');

const EventTokenSchema = new mongoose.Schema({
    event_type: String,
    api_key_hash: String,
    is_active: Boolean,
    created_at: Date,
    expires_at: Date,
    last_refreshed_at: Date,
    next_refresh_at: Date,
    auto_refresh_enabled: {
        type: Boolean,
        default: true
    }
}, { _id: false });

const ClientSchema = new mongoose.Schema({
    name: String,
    email: {
        type: String,
        unique: true
    },
    password: String,

    client_id: {
        type: String,
        unique: true
    },

    client_name: {
        type: String,
        required: true
    },

    event_tokens: {
        type: [EventTokenSchema],
        default: []
    },

    monthly_quota: {
        type: Number,
        default: 10000000
    },

    quota_used: {
        type: Number,
        default: 0
    },

    quota_reset_at: {
        type: Date,
        default: () => new Date()
    },

    per_user_rate_limit: {
        type: Number,
        default: 5000
    },

    global_rate_limit: {
        type: Number,
        default: 10000000
    },

    webhook_url: String,

    is_active: {
        type: Boolean,
        default: true
    },

    created_at: {
        type: Date,
        default: () => new Date()
    }

});

module.exports = mongoose.model('ApiClient', ClientSchema, 'api_clients');
