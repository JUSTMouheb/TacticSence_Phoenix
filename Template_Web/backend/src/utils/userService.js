const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

// Path to the users.json file
const usersFilePath = path.join(__dirname, '../../data/users.json');

// JWT secret key
const JWT_SECRET = 'your_jwt_secret_key';

// Helper function to read users from file
function readUsers() {
  try {
    if (!fs.existsSync(usersFilePath)) {
      console.log('Users file not found, creating new one');
      fs.writeFileSync(usersFilePath, JSON.stringify([]));
      return [];
    }
    
    const data = fs.readFileSync(usersFilePath, 'utf8');
    console.log('Read users file:', data.substring(0, 100) + '...');
    
    try {
      return JSON.parse(data);
    } catch (parseError) {
      console.error('Error parsing users JSON:', parseError);
      return [];
    }
  } catch (error) {
    console.error('Error reading users file:', error);
    return [];
  }
}

// Update the writeUsers function
function writeUsers(users) {
  try {
    // Check if the directory exists
    const dataDir = path.dirname(usersFilePath);
    if (!fs.existsSync(dataDir)) {
      console.log(`Creating data directory: ${dataDir}`);
      fs.mkdirSync(dataDir, { recursive: true });
    }
    
    // Write the users file
    fs.writeFileSync(usersFilePath, JSON.stringify(users, null, 2));
    console.log(`Successfully wrote users file: ${users.length} users`);
    return true;
  } catch (error) {
    console.error('Error writing users file:', error);
    return false;
  }
}

// Hash password
function hashPassword(password) {
  return crypto.createHash('sha256').update(password).digest('hex');
}

// User service functions
const userService = {
  // Register new user
  registerUser(userData) {
    const users = readUsers();
    
    // Check if email already exists
    if (users.find(user => user.email === userData.email)) {
      throw new Error('Email already registered');
    }
    
    // Create new user object
    const newUser = {
      id: `user-${Date.now()}`,
      email: userData.email,
      name: userData.name,
      role: userData.role,
      password: hashPassword(userData.password),
      verified: true, // Since they've gone through verification
      created: new Date().toISOString(),
      prefs: {
        filterPreferences: {
          regions: [],
          positions: [],
          specializations: []
        }
      }
    };
    
    // Add face image if provided (store as path or data URI)
    if (userData.faceImage) {
      newUser.faceImage = userData.faceImage;
    }
    
    // Add to users array
    users.push(newUser);
    
    // Write updated users to file
    if (writeUsers(users)) {
      // Create a copy without the password
      const { password, ...userWithoutPassword } = newUser;
      return userWithoutPassword;
    } else {
      throw new Error('Failed to save user');
    }
  },
  
  // Login user
  loginUser(email, password) {
    const users = readUsers();
    
    // Find user by email
    const user = users.find(user => user.email === email);
    if (!user) {
      throw new Error('Invalid credentials');
    }
    
    // Check password
    const hashedPassword = hashPassword(password);
    if (user.password !== hashedPassword) {
      throw new Error('Invalid credentials');
    }
    
    // Generate JWT token
    const token = jwt.sign({ userId: user.id }, JWT_SECRET, { expiresIn: '24h' });
    
    // Return user data without password
    const { password: _, ...userWithoutPassword } = user;
    return { user: userWithoutPassword, token };
  },
  
  // Get user by ID
  getUserById(userId) {
    const users = readUsers();
    const user = users.find(user => user.id === userId);
    
    if (!user) {
      return null;
    }
    
    // Return without password
    const { password, ...userWithoutPassword } = user;
    return userWithoutPassword;
  },
  
  // Update user preferences
  updateUserPreferences(userId, preferences) {
    const users = readUsers();
    const userIndex = users.findIndex(user => user.id === userId);
    
    if (userIndex === -1) {
      throw new Error('User not found');
    }
    
    // Update preferences
    users[userIndex].prefs = {
      ...users[userIndex].prefs,
      filterPreferences: preferences
    };
    
    // Write updated users to file
    if (writeUsers(users)) {
      return users[userIndex].prefs;
    } else {
      throw new Error('Failed to update preferences');
    }
  },
  
  // Verify token and get user
  verifyToken(token) {
    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      return this.getUserById(decoded.userId);
    } catch (error) {
      return null;
    }
  }
};

module.exports = userService;