"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.RecommendationService = void 0;
const common_1 = require("@nestjs/common");
const mongoose_1 = require("@nestjs/mongoose");
const mongoose_2 = require("mongoose");
let RecommendationService = class RecommendationService {
    constructor(userModel, recommendationProfileModel) {
        this.userModel = userModel;
        this.recommendationProfileModel = recommendationProfileModel;
    }
    async getPeopleRecommendations(userId, role, limit = 10) {
        var _a, _b;
        const userProfile = await this.recommendationProfileModel.findOne({ userId });
        if (!userProfile) {
            return [];
        }
        let targetRoles = [];
        if (role === 'Player') {
            targetRoles = ['Agent', 'Club Staff'];
        }
        else if (role === 'Agent') {
            targetRoles = ['Player', 'Club Staff'];
        }
        else if (role === 'Club Staff') {
            targetRoles = ['Player', 'Agent'];
        }
        const recommendations = await this.userModel.aggregate([
            {
                $match: {
                    _id: { $nin: [userId, ...(userProfile.connections || [])] },
                    role: { $in: targetRoles }
                }
            },
            {
                $lookup: {
                    from: 'recommendationProfiles',
                    localField: '_id',
                    foreignField: 'userId',
                    as: 'recProfile'
                }
            },
            {
                $unwind: {
                    path: '$recProfile',
                    preserveNullAndEmptyArrays: true
                }
            },
            {
                $addFields: {
                    mutualConnections: {
                        $size: {
                            $setIntersection: ['$recProfile.connections', userProfile.connections || []]
                        }
                    },
                    locationScore: {
                        $cond: [
                            { $eq: ['$location.country', (_a = userProfile.location) === null || _a === void 0 ? void 0 : _a.country] },
                            { $cond: [{ $eq: ['$location.city', (_b = userProfile.location) === null || _b === void 0 ? void 0 : _b.city] }, 10, 5] },
                            0
                        ]
                    }
                }
            },
            {
                $sort: {
                    mutualConnections: -1,
                    locationScore: -1
                }
            },
            {
                $limit: limit
            }
        ]);
        return recommendations;
    }
    async recordProfileView(viewerId, viewedId) {
        await this.recommendationProfileModel.findOneAndUpdate({ userId: viewerId }, {
            $push: {
                viewedProfiles: {
                    userId: viewedId,
                    timestamp: new Date()
                }
            }
        }, { upsert: true });
        await this.recommendationProfileModel.findOneAndUpdate({
            userId: viewerId,
            'interactionScore.userId': viewedId
        }, {
            $inc: { 'interactionScore.$.score': 1 }
        });
        await this.recommendationProfileModel.findOneAndUpdate({
            userId: viewerId,
            'interactionScore.userId': { $ne: viewedId }
        }, {
            $push: {
                interactionScore: {
                    userId: viewedId,
                    score: 1
                }
            }
        });
    }
    async recordConnection(userId1, userId2) {
        await this.recommendationProfileModel.findOneAndUpdate({ userId: userId1 }, { $addToSet: { connections: userId2 } }, { upsert: true });
        await this.recommendationProfileModel.findOneAndUpdate({ userId: userId2 }, { $addToSet: { connections: userId1 } }, { upsert: true });
    }
};
RecommendationService = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, mongoose_1.InjectModel)('User')),
    __param(1, (0, mongoose_1.InjectModel)('RecommendationProfile')),
    __metadata("design:paramtypes", [mongoose_2.Model,
        mongoose_2.Model])
], RecommendationService);
exports.RecommendationService = RecommendationService;
//# sourceMappingURL=recommendation.service.js.map