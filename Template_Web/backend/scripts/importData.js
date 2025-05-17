const fs = require('fs');
const csv = require('csv-parser');
const mongoose = require('mongoose');
const config = require('config');
const Player = require('../models/Player');
const Club = require('../models/Club');
const Agent = require('../models/Agent');

// Connect to MongoDB
mongoose.connect(config.get('mongoURI'), {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  useCreateIndex: true,
  useFindAndModify: false
});

// Import Players
const importPlayers = () => {
  const results = [];
  
  fs.createReadStream('../data/Players_Dataset.csv')
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', async () => {
      try {
        // Clear existing data
        await Player.deleteMany({});
        
        // Format and save new data
        const players = results.map(row => {
          return {
            full_name: row.full_name,
            nationality: row.nationality,
            age: parseInt(row.age) || 0,
            position: row.position,
            club: row.club,
            market_value_eur: parseFloat(row.market_value_eur) || 0,
            goals: parseInt(row.goals) || 0,
            assists: parseInt(row.assists) || 0,
            contract_end: row.contract_end
          };
        });
        
        await Player.insertMany(players);
        console.log(`${players.length} players imported successfully`);
      } catch (error) {
        console.error('Error importing players:', error);
      }
    });
};

// Import Clubs
const importClubs = () => {
  // Similar implementation for clubs
};

// Import Agents
const importAgents = () => {
  // Similar implementation for agents
};

// Run imports
importPlayers();
importClubs();
importAgents();