import { Client, Account } from 'appwrite';

// Initialize Appwrite client
const client = new Client()
  .setEndpoint('https://fra.cloud.appwrite.io/v1')  // Updated to Frankfurt region endpoint
  .setProject('6821c8a800391d2a4977'); // Project ID without the 'project-fra-' prefix

// Initialize Account service
export const account = new Account(client);