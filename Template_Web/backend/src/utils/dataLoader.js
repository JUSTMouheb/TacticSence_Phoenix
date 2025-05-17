const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

// Map to store loaded datasets
const datasets = {};

/**
 * Load data from a CSV file
 * @param {string} filename - CSV file name (without path)
 * @returns {Promise<Array>} - Array of parsed objects
 */
function loadCsvData(filename) {
  const results = [];
  const filePath = path.join(__dirname, '../../data', filename);
  
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) {
      console.error(`File not found: ${filePath}`);
      return reject(new Error(`File not found: ${filename}`));
    }
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => {
        console.log(`Loaded ${results.length} records from ${filename}`);
        resolve(results);
      })
      .on('error', (error) => {
        console.error(`Error loading ${filename}:`, error);
        reject(error);
      });
  });
}

/**
 * Load all datasets from the data directory
 */
async function loadAllDatasets() {
  try {
    const dataDir = path.join(__dirname, '../../data');
    const files = fs.readdirSync(dataDir);
    
    for (const file of files) {
      if (file.endsWith('.csv')) {
        try {
          const datasetName = file.replace('.csv', '');
          datasets[datasetName] = await loadCsvData(file);
          console.log(`Dataset '${datasetName}' loaded successfully`);
        } catch (error) {
          console.error(`Failed to load dataset ${file}:`, error);
        }
      }
    }
    
    console.log('All datasets loaded successfully');
    return datasets;
  } catch (error) {
    console.error('Error loading datasets:', error);
    throw error;
  }
}

/**
 * Get a specific dataset by name
 */
function getDataset(name) {
  return datasets[name] || [];
}

/**
 * Get all loaded datasets
 */
function getAllDatasets() {
  return datasets;
}

module.exports = {
  loadCsvData,
  loadAllDatasets,
  getDataset,
  getAllDatasets
};