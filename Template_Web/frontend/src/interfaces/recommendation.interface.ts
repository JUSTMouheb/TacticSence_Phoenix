export interface Recommendation {
  id: string;
  entityId: string;
  entityType: 'player' | 'club' | 'agent' | 'staff' | 'service';
  name: string;
  image?: string;
  description: string;
  subtext?: string;
  stats?: string;
  matchPercentage: number;
  matchReasons: string[];
  connected: boolean;
  connecting?: boolean;
}