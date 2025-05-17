const express = require('express');
const router = express.Router();
const { getDataset } = require('../../utils/dataLoader');

/**
 * @route   GET api/recommendations
 * @desc    Get recommendations for logged in user
 * @access  Private
 */
router.get('/', async (req, res) => {
  try {
    // Get the user ID from query params or auth token
    const userId = req.query.userId || 'default';
    
    // Generate recommendations from your datasets
    const recommendations = await generateRecommendations(userId);
    
    // Return the recommendations
    res.json(recommendations);
  } catch (err) {
    console.error('Error fetching recommendations:', err.message);
    res.status(500).send('Server Error');
  }
});

/**
 * Generate recommendations based on real datasets
 */
async function generateRecommendations(userId) {
  // Load datasets
  const players = getDataset('players');
  const clubs = getDataset('clubs');
  const agents = getDataset('agents');
  
  // If datasets are empty, return empty array
  if (!players.length && !clubs.length && !agents.length) {
    console.warn('No data found in datasets');
    return [];
  }
  
  // Convert datasets to recommendation format
  const recommendations = [
    ...players.slice(0, 10).map((player, index) => ({
      id: player.id || `player-${index}`,
      entityId: player.id || `player-${index}`,
      entityType: 'player',
      name: player.name || player.full_name || `Unknown Player ${index}`,
      image: player.image || player.photo || null,
      description: `${player.position || 'Player'} • ${player.club || player.team || 'Unknown Club'}`,
      subtext: player.age ? `${player.age} years • ${player.nationality || player.country || 'Unknown'}` : null,
      matchPercentage: calculateMatchScore(player, userId),
      matchReasons: generateMatchReasons(player, 'player'),
      connected: false
    })),
    ...clubs.slice(0, 5).map((club, index) => ({
      id: club.id || `club-${index}`,
      entityId: club.id || `club-${index}`,
      entityType: 'club',
      name: club.name || `Unknown Club ${index}`,
      image: club.image || club.logo || null,
      description: `${club.league || 'League'} • ${club.country || 'Unknown Country'}`,
      subtext: club.seeking ? `Looking for: ${club.seeking}` : null,
      matchPercentage: calculateMatchScore(club, userId),
      matchReasons: generateMatchReasons(club, 'club'),
      connected: false
    })),
    ...agents.slice(0, 5).map((agent, index) => ({
      id: agent.id || `agent-${index}`,
      entityId: agent.id || `agent-${index}`,
      entityType: 'agent',
      name: agent.name || `Unknown Agent ${index}`,
      image: agent.image || agent.photo || null,
      description: `Agent • ${agent.specialization || 'Various specializations'}`,
      subtext: agent.success_rate ? `${agent.success_rate}% success rate • ${agent.experience || '0'} years experience` : null,
      matchPercentage: calculateMatchScore(agent, userId),
      matchReasons: generateMatchReasons(agent, 'agent'),
      connected: false
    }))
  ];
  
  return recommendations;
}

/**
 * Calculate match score for an entity and user
 */
function calculateMatchScore(entity, userId) {
  // You would implement your actual matching algorithm here
  // For now, just return a random score between 70-98
  return Math.floor(Math.random() * 28) + 70;
}

/**
 * Generate match reasons for recommendations
 */
function generateMatchReasons(entity, type) {
  // Generate reasons based on entity type and properties
  const reasons = [];
  
  if (type === 'player') {
    if (entity.position) reasons.push(`Plays as ${entity.position}`);
    if (entity.club) reasons.push(`Currently at ${entity.club}`);
    if (entity.nationality) reasons.push(`Based in your region (${entity.nationality})`);
  } else if (type === 'club') {
    if (entity.league) reasons.push(`Plays in ${entity.league}`);
    if (entity.seeking) reasons.push(`Looking for players in your position`);
  } else if (type === 'agent') {
    if (entity.specialization) reasons.push(`Specializes in ${entity.specialization}`);
    if (entity.success_rate) reasons.push(`${entity.success_rate}% success rate with similar players`);
  }
  
  // Add generic reasons if needed
  if (reasons.length < 2) {
    reasons.push('Profile matches your interests');
  }
  
  return reasons;
}

module.exports = router;