const express = require('express');
const router = express.Router();
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

// Users JSON file path
const usersFilePath = path.join(__dirname, '../../../data/users.json');

// Middleware to read users from JSON file
const readUsers = () => {
  try {
    if (!fs.existsSync(usersFilePath)) {
      // Create file if it doesn't exist
      fs.writeFileSync(usersFilePath, JSON.stringify([], null, 2));
      return [];
    }
    const data = fs.readFileSync(usersFilePath, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    console.error('Error reading users file:', error);
    return [];
  }
};

// Middleware to write users to JSON file
const writeUsers = (users) => {
  try {
    fs.writeFileSync(usersFilePath, JSON.stringify(users, null, 2));
    return true;
  } catch (error) {
    console.error('Error writing to users file:', error);
    return false;
  }
};

// Auth middleware - DEFINE ONCE HERE
const authMiddleware = (req, res, next) => {
  // Get token from header
  const token = req.header('x-auth-token');
  
  // Check if no token
  if (!token) {
    return res.status(401).json({ msg: 'No token, authorization denied' });
  }
  
  // Verify token
  try {
    const decoded = jwt.verify(token, 'your_jwt_secret');
    req.user = decoded;
    next();
  } catch (err) {
    res.status(401).json({ msg: 'Token is not valid' });
  }
};

// @route   POST api/auth/register
// @desc    Register a user
// @access  Public
router.post('/register', (req, res) => {
  try {
    console.log('Registration request received');
    const { name, email, password, role } = req.body;
    
    // Basic validation
    if (!name || !email || !password || !role) {
      console.log('Missing required fields:', { name: !!name, email: !!email, password: !!password, role: !!role });
      return res.status(400).json({ msg: 'Please provide all required fields' });
    }
    
    // Read existing users
    const users = readUsers();
    
    // Check if user already exists
    const existingUser = users.find(user => user.email === email);
    if (existingUser) {
      console.log('User already exists with email:', email);
      return res.status(400).json({ msg: 'User already exists with that email' });
    }
    
    // Create new user object with hashed password
    const salt = bcrypt.genSaltSync(10);
    const hashedPassword = bcrypt.hashSync(password, salt);
    
    const newUser = {
      id: 'user_' + Date.now(),
      name,
      email,
      role,
      password: hashedPassword,
      createdAt: new Date().toISOString()
    };
    
    // Add user to array and write to file
    users.push(newUser);
    if (!writeUsers(users)) {
      return res.status(500).json({ msg: 'Error saving user data' });
    }
    
    // Create JWT token
    const token = jwt.sign(
      { id: newUser.id, role: newUser.role },
      'your_jwt_secret', // Replace with a proper secret from config
      { expiresIn: '1d' }
    );
    
    // Return success with token and user (without password)
    const { password: _, ...userWithoutPassword } = newUser;
    
    console.log('User registered successfully:', userWithoutPassword.email);
    res.status(201).json({
      token,
      user: userWithoutPassword
    });
    
  } catch (error) {
    console.error('Registration error:', error.message);
    res.status(500).json({ msg: 'Server error during registration' });
  }
});

// @route   POST api/auth/login
// @desc    Login a user
// @access  Public
router.post('/login', (req, res) => {
  try {
    const { email, password } = req.body;
    
    // Basic validation
    if (!email || !password) {
      return res.status(400).json({ msg: 'Please provide email and password' });
    }
    
    // Read users from file
    const users = readUsers();
    
    // Find user by email
    const user = users.find(u => u.email === email);
    if (!user) {
      return res.status(400).json({ msg: 'Invalid credentials' });
    }
    
    // Check password
    const isMatch = bcrypt.compareSync(password, user.password);
    if (!isMatch) {
      return res.status(400).json({ msg: 'Invalid credentials' });
    }
    
    // Create JWT token
    const token = jwt.sign(
      { id: user.id, role: user.role },
      'your_jwt_secret', // Replace with a proper secret from config
      { expiresIn: '1d' }
    );
    
    // Return token and user (without password)
    const { password: _, ...userWithoutPassword } = user;
    res.json({
      token,
      user: userWithoutPassword
    });
    
  } catch (error) {
    console.error('Login error:', error.message);
    res.status(500).json({ msg: 'Server error' });
  }
});

// REMOVE THE SECOND DECLARATION OF authMiddleware HERE
// It was duplicated, which caused the SyntaxError

// @route   GET api/auth/user
// @desc    Get user by token
// @access  Private
router.get('/user', authMiddleware, (req, res) => {
  try {
    const users = readUsers();
    const user = users.find(u => u.id === req.user.id);
    
    if (!user) {
      return res.status(404).json({ msg: 'User not found' });
    }
    
    // Return without password
    const { password, ...userWithoutPassword } = user;
    res.json(userWithoutPassword);
  } catch (error) {
    res.status(500).json({ msg: 'Server error' });
  }
});

// Export both the router and the middleware
module.exports = { router, authMiddleware };