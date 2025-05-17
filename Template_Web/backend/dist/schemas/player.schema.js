"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongoose_1 = __importDefault(require("mongoose"));
const PlayerSchema = new mongoose_1.default.Schema({
    full_name: {
        type: String,
        required: true
    },
    nationality: String,
    age: Number,
    position: String,
    club: String,
    market_value_eur: Number,
    goals: Number,
    assists: Number,
    contract_end: String,
    profile_image: String,
    skill_level: Number,
    playing_style: [String],
    userId: {
        type: mongoose_1.default.Schema.Types.ObjectId,
        ref: 'User'
    }
});
module.exports = mongoose_1.default.model('Player', PlayerSchema);
//# sourceMappingURL=player.schema.js.map