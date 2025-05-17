const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');

// In-memory connections store (in a real app, this would be in a database)
const connections = [];

/**
 * @route   POST api/connections
 * @desc    Create a new connection between user and entity
 * @access  Private
 */
router.post('/', (req, res) => {
  try {
    const { userId, entityId } = req.body;
    
    // Validate input
    if (!userId || !entityId) {
      return res.status(400).json({ msg: 'User ID and entity ID are required' });
    }
    
    // Check if connection already exists
    const existingConnection = connections.find(
      c => c.userId === userId && c.entityId === entityId
    );
    
    if (existingConnection) {
      return res.status(400).json({ msg: 'Connection already exists' });
    }
    
    // Create new connection
    const newConnection = {
      id: Date.now().toString(),
      userId,
      entityId,
      createdAt: new Date().toISOString()
    };
    
    connections.push(newConnection);
    console.log(`New connection created: User ${userId} -> Entity ${entityId}`);
    
    res.status(201).json(newConnection);
  } catch (err) {
    console.error('Error creating connection:', err.message);
    res.status(500).send('Server Error');
  }
});

/**
 * @route   DELETE api/connections/:userId/:entityId
 * @desc    Delete a connection between user and entity
 * @access  Private
 */
router.delete('/:userId/:entityId', (req, res) => {
  try {
    const { userId, entityId } = req.params;
    
    // Find connection index
    const connectionIndex = connections.findIndex(
      c => c.userId === userId && c.entityId === entityId
    );
    
    if (connectionIndex === -1) {
      return res.status(404).json({ msg: 'Connection not found' });
    }
    
    // Remove connection
    connections.splice(connectionIndex, 1);
    console.log(`Connection removed: User ${userId} -> Entity ${entityId}`);
    
    res.json({ msg: 'Connection removed' });
  } catch (err) {
    console.error('Error removing connection:', err.message);
    res.status(500).send('Server Error');
  }
});
// Register new user
router.post('/register', async (req, res) => {
  try {
    const { name, email, password, role } = req.body;
    
    // Log more details for debugging
    console.log('Register attempt:', { 
      hasName: !!name, 
      hasEmail: !!email, 
      hasPassword: !!password, 
      role 
    });
    
    // More detailed validation
    const missingFields = [];
    if (!name) missingFields.push('name');
    if (!email) missingFields.push('email');
    if (!password) missingFields.push('password');
    if (!role) missingFields.push('role');
    
    if (missingFields.length > 0) {
      console.log('Missing required fields:', missingFields);
      return res.status(400).json({ 
        msg: `Missing required fields: ${missingFields.join(', ')}` 
      });
    }
    
    // Validate inputs
    if (!name || !email || !password || !role) {
      console.log('Missing required fields');
      return res.status(400).json({ msg: 'Please enter all required fields' });
    }
    
    // Register the user
    const user = userService.registerUser({ name, email, password, role, faceImage });
    
    // Login the user after registration
    const authData = userService.loginUser(email, password);
    
    res.json(authData);
  } catch (error) {
    console.error('Registration error:', error.message);
    res.status(400).json({ msg: error.message });
  }
});
module.exports = router;