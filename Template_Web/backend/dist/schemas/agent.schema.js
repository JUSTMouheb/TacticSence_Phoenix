"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongoose_1 = __importDefault(require("mongoose"));
const AgentSchema = new mongoose_1.default.Schema({
    full_name: {
        type: String,
        required: true
    },
    nationality: String,
    specialization: [String],
    region: String,
    success_rate: Number,
    years_experience: Number,
    top_clients: [String],
    avg_contract_value: Number,
    userId: {
        type: mongoose_1.default.Schema.Types.ObjectId,
        ref: 'User'
    }
});
module.exports = mongoose_1.default.model('Agent', AgentSchema);
//# sourceMappingURL=agent.schema.js.map