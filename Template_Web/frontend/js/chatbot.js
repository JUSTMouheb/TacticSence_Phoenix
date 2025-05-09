class ChatbotService {
    constructor(apiUrl = 'http://localhost:5000/api/chatbot') {
        this.apiUrl = apiUrl;
        this.userId = 'user_' + new Date().getTime();
    }
    
    async sendQuery(query) {
        try {
            const response = await fetch(this.apiUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ 
                    query,
                    user_id: this.userId 
                }),
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error('Error querying chatbot:', error);
            return {
                message: 'Sorry, there was an error processing your request.',
                error: error.message
            };
        }
    }
    
    async healthCheck() {
        try {
            const response = await fetch(this.apiUrl.replace('/chatbot', '/health'), {
                method: 'GET'
            });
            return response.ok;
        } catch (error) {
            console.error('Health check failed:', error);
            return false;
        }
    }
}

// Initialize and export the chatbot service
const chatbotService = new ChatbotService();
export default chatbotService;