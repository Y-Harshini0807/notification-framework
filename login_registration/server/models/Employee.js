const mongoose = require('mongoose');
const EmployeeSchema = new mongoose.Schema({
    name: String,
    client_name: String,
    email: String,
    password: String,
    client_id: String,

    event_tokens: {
        type: Array,
        default: []   //  important
    },

    monthly_quota: {
        type: Number,
        default: 1000
    },

    quota_used: {
        type: Number,
        default: 0
    }
});

const EmployeeModel = mongoose.model("employees", EmployeeSchema)
module.exports = EmployeeModel