const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { loadAllDatasets } = require('./utils/dataLoader');

// Create Express app
const app = express();

// Create a router for user endpoints
const userRouter = express.Router();

// Function to read users from JSON file
const readUsers = () => {
  try {
    const usersFilePath = path.join(__dirname, '../data/users.json');
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

// Function to write users to JSON file
const writeUsers = (users) => {
  try {
    const usersFilePath = path.join(__dirname, '../data/users.json');
    fs.writeFileSync(usersFilePath, JSON.stringify(users, null, 2));
    return true;
  } catch (error) {
    console.error('Error writing to users file:', error);
    return false;
  }
};

// Make sure data directory exists
const dataDir = path.join(__dirname, '../data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir);
}

// Middleware
app.use(cors());
app.use(express.json());

// Add debug logging middleware
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// Define API Routes
const { router: authRouter, authMiddleware } = require('./routes/api/auth');
app.use('/api/auth', authRouter);
app.use('/api/connections', require('./routes/api/connections'));
app.use('/api/users', require('./routes/api/users'));

// Test API route
app.get('/api/test', (req, res) => {
  console.log('API test route hit');
  res.json({ msg: 'API is working!' });
});

// Add user endpoint to check if a user exists by ID
userRouter.get('/user/:id', (req, res) => {
  try {
    console.log(`Finding user with id: ${req.params.id}`);
    const users = readUsers();
    const user = users.find(u => u.id === req.params.id);
    
    if (!user) {
      return res.status(404).json({ msg: 'User not found' });
    }
    
    // Return without password
    const { password, ...userWithoutPassword } = user;
    res.json(userWithoutPassword);
  } catch (error) {
    console.error('Error fetching user:', error);
    res.status(500).json({ msg: 'Server error' });
  }
});

// Register the user router
app.use('/api', userRouter);

// Update user preferences route - commented out for now since userService isn't defined
/*
app.use('/api/users/:id/preferences', authMiddleware, (req, res) => {
  // Check if the user is attempting to modify their own preferences
  if (req.user.id !== req.params.id) {
    return res.status(403).json({ msg: 'Not authorized to modify other users' });
  }
  
  // Continue with the preferences update
  try {
    const { filterPreferences } = req.body;
    const updatedPrefs = userService.updateUserPreferences(req.user.id, filterPreferences);
    res.json(updatedPrefs);
  } catch (error) {
    res.status(400).json({ msg: error.message });
  }
});
*/

// Root route API response
app.get('/api', (req, res) => {
  res.json({ msg: 'TacticSense API Running' });
});

// IMPORTANT: Serve static files after API routes but before HTML routes
app.use(express.static(path.join(__dirname, '../../frontend/src')));

// Debug static serving
console.log(`Serving static files from: ${path.join(__dirname, '../../frontend/src')}`);

// HTML routes with debug logging
app.get('/dashboard.html', (req, res) => {
  console.log('Dashboard HTML route hit, serving dashboard.html');
  res.sendFile(path.join(__dirname, '../../frontend/src/dashboard.html'));
});

app.get('/agent-dashboard.html', (req, res) => {
  console.log('Agent dashboard HTML route hit');
  res.sendFile(path.join(__dirname, '../../frontend/src/agent-dashboard.html'));
});

app.get('/club-dashboard.html', (req, res) => {
  console.log('Club dashboard HTML route hit');
  res.sendFile(path.join(__dirname, '../../frontend/src/club-dashboard.html'));
});

app.get('/service-provider-dashboard.html', (req, res) => {
  console.log('Service provider dashboard HTML route hit');
  res.sendFile(path.join(__dirname, '../../frontend/src/service-provider-dashboard.html'));
});

// Root route - serves HTML response
app.get('/', (req, res) => {
  console.log('Root route hit, sending welcome message');
  res.send('TacticSense API Running');
});

// Load all datasets
loadAllDatasets()
  .then(() => {
    console.log('All datasets loaded successfully');
    
    // Start server
    const PORT = process.env.PORT || 5000;
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`Dashboard should be accessible at: http://localhost:${PORT}/dashboard.html`);
    });
  })
  .catch(err => {
    console.error('Failed to load datasets:', err);
    process.exit(1);
  });