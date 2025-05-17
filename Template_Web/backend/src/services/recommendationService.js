const Player = require('../models/Player');
const Club = require('../models/Club');
const Agent = require('../models/Agent');
const User = require('../models/User');
const Connection = require('../models/Connection');

// Recommendation engine for different user roles
class RecommendationService {
  
  async getRecommendationsForUser(userId) {
    try {
      const user = await User.findById(userId);
      if (!user) {
        throw new Error('User not found');
      }
      
      // Get existing connections to avoid recommending them
      const connections = await Connection.find({ userId });
      const connectionIds = connections.map(c => c.connectionId.toString());
      
      // Get recommendations based on role
      let recommendations = [];
      switch(user.role) {
        case 'Player':
          recommendations = await this.getRecommendationsForPlayer(user, connectionIds);
          break;
        case 'Agent':
          recommendations = await this.getRecommendationsForAgent(user, connectionIds);
          break;
        case 'Club Staff':
          recommendations = await this.getRecommendationsForClubStaff(user, connectionIds);
          break;
        case 'Service Provider':
          recommendations = await this.getRecommendationsForServiceProvider(user, connectionIds);
          break;
      }
      
      return recommendations;
    } catch (error) {
      console.error('Error getting recommendations:', error);
      throw error;
    }
  }
  
  async getRecommendationsForPlayer(user, connectionIds) {
    // Get player details if exists
    const playerProfile = await Player.findOne({ userId: user._id });
    
    // Recommendation strategy for players:
    // 1. Recommend clubs looking for players in their position
    // 2. Recommend agents specializing in their position
    
    const recommendations = [];
    
    // Get clubs that might need the player
    const clubs = await Club.find({
      _id: { $nin: connectionIds },
      // If player has a position, recommend clubs with matching needs
      ...(playerProfile?.position ? { current_needs: playerProfile.position } : {})
    }).limit(5);
    
    // Process club recommendations
    for (const club of clubs) {
      const matchPercentage = this.calculatePlayerClubMatch(playerProfile, club);
      const matchReasons = this.getPlayerClubMatchReasons(playerProfile, club);
      
      recommendations.push({
        _id: `rec_${recommendations.length + 1}`,
        entityId: club._id,
        entityType: 'club',
        name: club.name,
        image: club.logo,
        description: `${club.league} • ${club.country}`,
        subtext: club.current_needs?.length > 0 ? `Looking for: ${club.current_needs.join(', ')}` : undefined,
        matchPercentage,
        matchReasons
      });
    }
    
    // Get agents specializing in the player's position
    const agents = await Agent.find({
      _id: { $nin: connectionIds },
      ...(playerProfile?.position ? { specialization: playerProfile.position } : {})
    }).limit(5);
    
    // Process agent recommendations
    for (const agent of agents) {
      const matchPercentage = this.calculatePlayerAgentMatch(playerProfile, agent);
      const matchReasons = this.getPlayerAgentMatchReasons(playerProfile, agent);
      
      recommendations.push({
        _id: `rec_${recommendations.length + 1}`,
        entityId: agent._id,
        entityType: 'agent',
        name: agent.full_name,
        description: `Agent • ${agent.specialization.join(', ')}`,
        subtext: `${agent.success_rate}% success rate • ${agent.years_experience} years experience`,
        matchPercentage,
        matchReasons
      });
    }
    
    return recommendations;
  }
  
  async getRecommendationsForAgent(user, connectionIds) {
    // Get agent details if exists
    const agentProfile = await Agent.findOne({ userId: user._id });
    
    // Recommendation strategy for agents:
    // 1. Recommend players that need representation
    // 2. Recommend clubs looking for players in agent's specialization
    
    const recommendations = [];
    
    // Get players that match agent's specialization
    const players = await Player.find({
      _id: { $nin: connectionIds },
      ...(agentProfile?.specialization?.length > 0 ? { position: { $in: agentProfile.specialization } } : {})
    }).limit(5);
    
    // Process player recommendations
    for (const player of players) {
      const matchPercentage = this.calculateAgentPlayerMatch(agentProfile, player);
      const matchReasons = this.getAgentPlayerMatchReasons(agentProfile, player);
      
      recommendations.push({
        _id: `rec_${recommendations.length + 1}`,
        entityId: player._id,
        entityType: 'player',
        name: player.full_name,
        image: player.profile_image,
        description: `${player.position} • ${player.club}`,
        subtext: `${player.age} years • ${player.nationality}`,
        matchPercentage,
        matchReasons
      });
    }
    
    // Similar logic for club recommendations
    // ...
    
    return recommendations;
  }
  
  async getRecommendationsForClubStaff(user, connectionIds) {
    // Get club details if exists
    const clubProfile = await Club.findOne({ userId: user._id });
    
    // Recommendation strategy for club staff:
    // 1. Recommend players that match club needs
    // 2. Recommend agents with players in needed positions
    
    const recommendations = [];
    
    // Get players that match club needs
    const players = await Player.find({
      _id: { $nin: connectionIds },
      ...(clubProfile?.current_needs?.length > 0 ? { position: { $in: clubProfile.current_needs } } : {})
    }).limit(5);
    
    // Process player recommendations
    for (const player of players) {
      const matchPercentage = this.calculateClubPlayerMatch(clubProfile, player);
      const matchReasons = this.getClubPlayerMatchReasons(clubProfile, player);
      
      recommendations.push({
        _id: `rec_${recommendations.length + 1}`,
        entityId: player._id,
        entityType: 'player',
        name: player.full_name,
        image: player.profile_image,
        description: `${player.position} • ${player.club}`,
        subtext: `Market Value: €${this.formatNumber(player.market_value_eur)}`,
        matchPercentage,
        matchReasons
      });
    }
    
    // Similar logic for agent recommendations
    // ...
    
    return recommendations;
  }
  
  async getRecommendationsForServiceProvider(user, connectionIds) {
    // Implement service provider recommendations
    // ...
    return [];
  }
  
  // Match calculation methods
  calculatePlayerClubMatch(player, club) {
    if (!player || !club) return 50; // Base match percentage
    
    let score = 50;
    
    // Position match with club needs
    if (player.position && club.current_needs?.includes(player.position)) {
      score += 25;
    }
    
    // Nationality match
    if (player.nationality === club.country) {
      score += 15;
    }
    
    // Playing style match
    if (player.playing_style?.some(style => style === club.playing_style)) {
      score += 10;
    }
    
    return Math.min(score, 100);
  }
  
  getPlayerClubMatchReasons(player, club) {
    const reasons = [];
    
    if (player?.position && club.current_needs?.includes(player.position)) {
      reasons.push(`Club is looking for ${player.position} players`);
    }
    
    if (player?.nationality === club.country) {
      reasons.push(`Based in your home country (${club.country})`);
    }
    
    if (player?.playing_style?.some(style => style === club.playing_style)) {
      reasons.push(`Similar playing style`);
    }
    
    // Always add at least one reason
    if (reasons.length === 0) {
      reasons.push('Club with potential opportunities');
    }
    
    return reasons;
  }
  
  calculatePlayerAgentMatch(player, agent) {
    if (!player || !agent) return 50; // Base match percentage
    
    let score = 50;
    
    // Position specialization match
    if (player.position && agent.specialization?.includes(player.position)) {
      score += 25;
    }
    
    // Region/nationality match
    if (player.nationality === agent.nationality || player.nationality === agent.region) {
      score += 15;
    }
    
    // Experience points
    score += Math.min(agent.years_experience, 10);
    
    return Math.min(score, 100);
  }
  
  getPlayerAgentMatchReasons(player, agent) {
    const reasons = [];
    
    if (player?.position && agent.specialization?.includes(player.position)) {
      reasons.push(`Specializes in ${player.position} players`);
    }
    
    if (player?.nationality === agent.nationality || player?.nationality === agent.region) {
      reasons.push(`Works with ${player?.nationality} players`);
    }
    
    if (agent.years_experience > 5) {
      reasons.push(`${agent.years_experience} years of experience`);
    }
    
    if (agent.success_rate > 70) {
      reasons.push(`${agent.success_rate}% success rate with clients`);
    }
    
    // Always add at least one reason
    if (reasons.length === 0) {
      reasons.push('Active agent looking for new clients');
    }
    
    return reasons;
  }
  
  // Similar calculation methods for other entity matches
  // calculateAgentPlayerMatch, getAgentPlayerMatchReasons, etc.
  
  // Helper methods
  formatNumber(value) {
    if (!value) return '0';
    
    // Format number with commas
    return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  }
}

module.exports = new RecommendationService();