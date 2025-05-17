"use strict";
const mongoose = require('mongoose');
const UserSchema = new mongoose.Schema({
    email: {
        type: String,
        required: true,
        unique: true
    },
    name: {
        type: String,
        required: true
    },
    password: {
        type: String,
        required: true
    },
    role: {
        type: String,
        enum: ['Player', 'Agent', 'Club Staff', 'Service Provider'],
        required: true
    },
    profileCompleted: {
        type: Boolean,
        default: false
    },
    profileImage: {
        type: String
    },
    prefs: {
        avatar: String,
        filterPreferences: {
            regions: [String],
            positions: [String],
            leagues: [String],
            specializations: [String]
        },
        region: String
    },
    connections: [{
            type: mongoose.Schema.Types.ObjectId,
            ref: 'User'
        }],
    createdAt: {
        type: Date,
        default: Date.now
    }
});
module.exports = mongoose.model('User', UserSchema);
//# sourceMappingURL=user.schema.js.map