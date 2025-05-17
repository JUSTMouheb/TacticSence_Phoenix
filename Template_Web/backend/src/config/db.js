const mongoose = require('mongoose');
const config = require('config');

// If you don't have a config file, use this simplified version:
const connectDB = async () => {
  try {
    await mongoose.connect('mongodb://127.0.0.1:27017/tacticsense', {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });
    
    console.log('MongoDB Connected...');
  } catch (err) {
    console.error('MongoDB connection error:', err.message);
    // Exit process with failure
    process.exit(1);
  }
};

module.exports = connectDB;