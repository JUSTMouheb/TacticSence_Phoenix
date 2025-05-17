const express = require('express');
const router = express.Router();

// In-memory user preferences store
const userPreferences = {};

/**
 * @route   PUT api/users/:id/preferences
 * @desc    Update user preferences
 * @access  Private
 */
router.put('/:id/preferences', (req, res) => {
  try {
    const userId = req.params.id;
    const { filterPreferences } = req.body;
    
    // Validate input
    if (!filterPreferences) {
      return res.status(400).json({ msg: 'Filter preferences are required' });
    }
    
    // Update user preferences
    if (!userPreferences[userId]) {
      userPreferences[userId] = {};
    }
    
    userPreferences[userId].filterPreferences = filterPreferences;
    console.log(`Updated preferences for user ${userId}:`, filterPreferences);
    
    res.json({ msg: 'Preferences updated', preferences: userPreferences[userId] });
  } catch (err) {
    console.error('Error updating preferences:', err.message);
    res.status(500).send('Server Error');
  }
});

/**
 * @route   GET api/users/:id/preferences
 * @desc    Get user preferences
 * @access  Private
 */
router.get('/:id/preferences', (req, res) => {
  try {
    const userId = req.params.id;
    const preferences = userPreferences[userId] || { filterPreferences: {} };
    
    res.json(preferences);
  } catch (err) {
    console.error('Error fetching preferences:', err.message);
    res.status(500).send('Server Error');
  }
});

module.exports = router;