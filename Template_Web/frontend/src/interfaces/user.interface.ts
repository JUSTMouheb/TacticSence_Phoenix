export interface User {
  id: string;
  email: string;
  name: string;
  role: 'Player' | 'Agent' | 'Club Staff' | 'Service Provider';
  profileCompleted: boolean;
  profileImage?: string;
  prefs?: {
    avatar?: string;
    filterPreferences?: FilterPreferences;
    region?: string;
    connections: string[];
  };
}

export interface FilterPreferences {
  regions?: string[];
  positions?: string[];
  leagues?: string[];
  specializations?: string[];
}