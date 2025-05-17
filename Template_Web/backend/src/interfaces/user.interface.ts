import { Document } from 'mongoose';

export interface UserDocument extends Document {
  name: string;
  email: string;
  role: 'Player' | 'Agent' | 'Club Staff';
  profileImage?: string;
  location?: {
    country: string;
    city: string;
  };
  verified: boolean;
  skills?: string[];
  createdAt?: Date;
  updatedAt?: Date;
}