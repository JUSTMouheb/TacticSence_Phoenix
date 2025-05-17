"use strict";
const recommendationService = require('../services/recommendationService');
const Connection = require('../models/Connection');
const User = require('../models/User');
exports.getRecommendations = async (req, res) => {
    try {
        const userId = req.user.id;
        const recommendations = await recommendationService.getRecommendationsForUser(userId);
        const connections = await Connection.find({ userId });
        const connectionMap = connections.reduce((map, conn) => {
            map[conn.connectionId.toString()] = conn;
            return map;
        }, {});
        const enhancedRecommendations = recommendations.map((rec) => (Object.assign(Object.assign({}, rec), { connected: connectionMap[rec.entityId.toString()] !== undefined })));
        res.json(enhancedRecommendations);
    }
    catch (error) {
        console.error('Error fetching recommendations:', error);
        res.status(500).json({ message: 'Error fetching recommendations' });
    }
};
exports.saveConnection = async (req, res) => {
    try {
        const userId = req.user.id;
        const { entityId, entityType } = req.body;
        if (!entityId || !entityType) {
            return res.status(400).json({ message: 'Missing required fields' });
        }
        const existingConnection = await Connection.findOne({ userId, connectionId: entityId });
        if (existingConnection) {
            return res.status(400).json({ message: 'Connection already exists' });
        }
        const connection = new Connection({
            userId,
            connectionId: entityId,
            entityType,
            status: 'accepted'
        });
        await connection.save();
        await User.findByIdAndUpdate(userId, {
            $push: { connections: entityId }
        });
        res.json({ success: true, connection });
    }
    catch (error) {
        console.error('Error saving connection:', error);
        res.status(500).json({ message: 'Error saving connection' });
    }
};
exports.removeConnection = async (req, res) => {
    try {
        const userId = req.user.id;
        const { entityId } = req.params;
        await Connection.findOneAndDelete({ userId, connectionId: entityId });
        await User.findByIdAndUpdate(userId, {
            $pull: { connections: entityId }
        });
        res.json({ success: true });
    }
    catch (error) {
        console.error('Error removing connection:', error);
        res.status(500).json({ message: 'Error removing connection' });
    }
};
exports.saveUserPreferences = async (req, res) => {
    try {
        const userId = req.user.id;
        const { filterPreferences } = req.body;
        await User.findByIdAndUpdate(userId, {
            'prefs.filterPreferences': filterPreferences
        });
        res.json({ success: true });
    }
    catch (error) {
        console.error('Error saving preferences:', error);
        res.status(500).json({ message: 'Error saving preferences' });
    }
};
//# sourceMappingURL=recommendation.controller.js.map