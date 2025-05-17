"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongoose_1 = __importDefault(require("mongoose"));
const ConnectionSchema = new mongoose_1.default.Schema({
    userId: {
        type: mongoose_1.default.Schema.Types.ObjectId,
        ref: 'User',
        required: true
    },
    connectionId: {
        type: mongoose_1.default.Schema.Types.ObjectId,
        required: true
    },
    entityType: {
        type: String,
        enum: ['player', 'club', 'agent', 'staff', 'service'],
        required: true
    },
    status: {
        type: String,
        enum: ['pending', 'accepted', 'rejected'],
        default: 'pending'
    },
    createdAt: {
        type: Date,
        default: Date.now
    }
});
module.exports = mongoose_1.default.model('Connection', ConnectionSchema);
//# sourceMappingURL=connection.schema.js.map