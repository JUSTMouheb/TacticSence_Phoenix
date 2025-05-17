import { Model } from 'mongoose';
import { RecommendationProfile } from '../models/recommendation-profile.model';
import { User } from '../models/user.model';
export declare class RecommendationService {
    private userModel;
    private recommendationProfileModel;
    constructor(userModel: Model<User>, recommendationProfileModel: Model<RecommendationProfile>);
    getPeopleRecommendations(userId: string, role: string, limit?: number): Promise<any[]>;
    recordProfileView(viewerId: string, viewedId: string): Promise<void>;
    recordConnection(userId1: string, userId2: string): Promise<void>;
}
