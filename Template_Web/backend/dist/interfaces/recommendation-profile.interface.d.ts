import { Document } from 'mongoose';
export interface RecommendationProfileDocument extends Document {
    userId: string;
    role: 'Player' | 'Agent' | 'Club Staff';
    skills: string[];
    location?: {
        country: string;
        city: string;
    };
    connections: string[];
    viewedProfiles: {
        userId: string;
        timestamp: Date;
    }[];
    interactionScore: {
        userId: string;
        score: number;
    }[];
}
