from flask import Flask, request, jsonify
import os
import sys
import pandas as pd
from pathlib import Path
import re
import traceback
from collections import defaultdict

# Add the chatbot directory to the Python path
chatbot_path = r"C:\Modeling Notebooks\Chatbot_model\football-stakeholders-chatbot"
sys.path.append(chatbot_path)

# Now import the FootballChatbot class
try:
    print(f"Trying to import FootballChatbot from {chatbot_path}")
    from test_chatbot import FootballChatbot
    print("Successfully imported FootballChatbot")
except ImportError as e:
    print(f"ImportError: {e}")
    try:
        # Try alternative imports if the first one fails
        from app.chatbot import FootballChatbot
        print("Imported FootballChatbot from app.chatbot")
    except ImportError:
        print("Failed to import FootballChatbot. Make sure the class is defined properly.")
        sys.exit(1)

app = Flask(__name__)

# Enable CORS for frontend communication
try:
    from flask_cors import CORS
    CORS(app)
    print("CORS enabled for Flask app")
except ImportError:
    print("Warning: flask_cors not installed. CORS support disabled.")

# Add a context store to remember conversations
user_contexts = {}

# Define the default context structure
def get_default_context():
    return {
        'last_query': None,
        'last_response': None,
        'last_entity_type': None,
        'last_result_count': 0,
        'last_results': None
    }

# Function to clean dataframe for JSON serialization
def clean_dataframe_for_json(df):
    """Clean a DataFrame to make it JSON-serializable by handling NaN values"""
    # Make a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Replace NaN/None values with proper placeholders based on column type
    for column in df_clean.columns:
        # Replace NaN in numeric columns with 0
        if df_clean[column].dtype.kind in 'fiubc':  # float, int, uint, bool, complex
            df_clean[column] = df_clean[column].fillna(0)
        # Replace NaN in other columns (like strings) with empty string
        else:
            df_clean[column] = df_clean[column].fillna('')
    
    return df_clean

# Load datasets
def load_datasets():
    """Load all necessary datasets"""
    datasets = {}
    
    # Define dataset paths - use the path from your test_chatbot.py
    base_path = r"C:\Modeling Notebooks"
    
    dataset_files = {
        'players': os.path.join(base_path, 'Players_Dataset.csv'),
        'clubs': os.path.join(base_path, 'Clubs_Dataset.csv'),
        'managers_staff': os.path.join(base_path, 'Managers_Staff_Dataset.csv'),
        'player_agents': os.path.join(base_path, 'Players_Agents_Dataset.csv'),
        'recruiting_agents': os.path.join(base_path, 'Recruiting_Agents_Dataset.csv'),
        'service_providers': os.path.join(base_path, 'Service_Providers_Dataset.csv'),
        'sporting_agencies': os.path.join(base_path, 'Sporting_management_agencies_Dataset.csv'),
        'communication_boxes': os.path.join(base_path, 'Communication_Boxes_Dataset.csv'),
        'equipment_suppliers': os.path.join(base_path, 'Equipement_Suppliers_Dataset.csv'),
        'sports_clothing': os.path.join(base_path, 'Sports_Clothing_Brands_Dataset.csv'),
        'travel_agencies': os.path.join(base_path, 'Traveling_agencies_Dataset.csv'),
        'sponsors': os.path.join(base_path, 'Sponsors_Dataset.csv')
    }
    
    print(f"Looking for datasets in: {base_path}")
    
    # Try different encodings for problematic files
    encodings = ['utf-8', 'latin1', 'cp1252', 'ISO-8859-1']
    
    # Load each dataset if the file exists
    for key, file_path in dataset_files.items():
        print(f"Checking for {key} dataset at: {file_path}")
        if os.path.exists(file_path):
            loaded = False
            
            # Try different encodings
            for encoding in encodings:
                try:
                    datasets[key] = pd.read_csv(file_path, encoding=encoding)
                    print(f"Loaded {key} dataset using {encoding} encoding")
                    loaded = True
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"Error loading {file_path}: {e}")
                    break
            
            if not loaded:
                print(f"Failed to load {file_path} with any encoding")
        else:
            print(f"Warning: {file_path} not found")
    
    return datasets

# Initialize chatbot
print("Loading datasets...")
datasets = load_datasets()

print("Initializing chatbot...")
try:
    chatbot = FootballChatbot(datasets)
    print("Chatbot initialized successfully!")
except Exception as e:
    print(f"Error initializing chatbot: {e}")
    traceback.print_exc()
    sys.exit(1)

# Helper functions for context handling
def is_followup_query(query):
    """Detect if this is a follow-up question"""
    query = query.lower().strip()
    followup_patterns = [
        r'^(show|list|display) (them|those|these|results)$',
        r'^can (you )?(show|list|display) (them|those|these|results)(\s|$)',
        r'^(who|what) are they(\s|$)',
        r'^tell me (about|more)(\s|$)',
        r'^(give|show) me (more|the|all) (details|results|data)(\s|$)',
        r'^(could|can) (you )?(list|show) (them|those|these|the players|the results)(\s|$)'
    ]
    return any(re.search(pattern, query) for pattern in followup_patterns)

def is_website_info_query(query):
    """Detect if this is asking about the website/system"""
    query = query.lower().strip()
    info_patterns = [
        r'what (is|about) (this|the|your) (website|site|system|application|app|project)(\s|$)',
        r'(tell|explain) (me )?(about|what is) (this|the) (website|site|system|app|project)(\s|$)',
        r'what do(es)? (this|the|your) (website|site|system|app|project) do(\s|$)',
        r'what (is|are) tacticsense(\s|$)',
        r'tell me about tacticsense(\s|$)'
    ]
    return any(re.search(pattern, query) for pattern in info_patterns)

def is_africa_football_query(query):
    """Detect if this is asking about football in Africa"""
    query = query.lower().strip()
    africa_patterns = [
        r'(football|soccer) (in )?africa',
        r'african (football|soccer)',
        r'(tell|explain) (me )?(about) (football|soccer) in africa',
        r'african (players|clubs|teams)'
    ]
    return any(re.search(pattern, query) for pattern in africa_patterns)

def handle_followup_query(query, context):
    """Handle a follow-up question based on context"""
    last_entity_type = context['last_entity_type']
    last_results = context['last_results']
    
    if last_results is None or len(last_results) == 0:
        return jsonify({'message': "I don't have any previous results to show you."})
    
    # Limit to first 10 results for display
    results_to_show = last_results.head(10) if hasattr(last_results, 'head') else last_results[:10]
    
    # Convert to dict if it's a DataFrame
    if hasattr(results_to_show, 'to_dict'):
        # Clean the dataframe first
        clean_results = clean_dataframe_for_json(results_to_show)
        results_dict = clean_results.to_dict(orient='records')
    else:
        results_dict = results_to_show
    
    # Format response based on entity type
    if last_entity_type == 'players':
        player_info = []
        for idx, player in enumerate(results_dict, 1):
            name = player.get('name', player.get('full_name', 'Unknown'))
            age = player.get('age', 'N/A')
            club = player.get('club', player.get('current_club', 'Unknown'))
            country = player.get('nationality', player.get('country', 'N/A'))
            player_info.append(f"{idx}. {name} (Age: {age}, Club: {club}, Nationality: {country})")
        
        response_details = '\n'.join(player_info)
        
        return jsonify({
            'message': f"Here are some of the {context['last_result_count']} players from your previous query:",
            'details': response_details,
            'result_data': results_dict
        })
    elif last_entity_type == 'clubs':
        club_info = []
        for idx, club in enumerate(results_dict, 1):
            name = club.get('name', club.get('club_name', 'Unknown'))
            country = club.get('country', 'Unknown')
            league = club.get('league', 'N/A')
            club_info.append(f"{idx}. {name} ({country}, League: {league})")
        
        response_details = '\n'.join(club_info)
        
        return jsonify({
            'message': f"Here are some of the {context['last_result_count']} clubs from your previous query:",
            'details': response_details,
            'result_data': results_dict
        })
    elif last_entity_type == 'sponsors':
        sponsor_info = []
        for idx, sponsor in enumerate(results_dict, 1):
            name = sponsor.get('name', sponsor.get('company_name', 'Unknown'))
            industry = sponsor.get('industry', 'Various')
            sponsor_info.append(f"{idx}. {name} (Industry: {industry})")
        
        response_details = '\n'.join(sponsor_info)
        
        return jsonify({
            'message': f"Here are some of the {context['last_result_count']} sponsors from your previous query:",
            'details': response_details,
            'result_data': results_dict
        })
    else:
        # Generic handling for other entity types
        return jsonify({
            'message': f"Here are results from your previous query:",
            'result_data': results_dict
        })
def get_general_football_info(query):
    """Handle general football questions"""
    football_knowledge = {
        'best player': "Many consider Lionel Messi and Cristiano Ronaldo to be the best players of the modern era, with players like Pelé and Maradona among the all-time greats. In Africa, Mohamed Salah (Egypt), Sadio Mané (Senegal), and Victor Osimhen (Nigeria) are among the current top stars.",
        'world cup': "The FIFA World Cup is held every four years. The most recent World Cup was in Qatar in 2022, won by Argentina. The next World Cup will be in 2026, hosted by the United States, Mexico, and Canada. Africa has 5-9 qualification spots for the World Cup.",
        'champions league': "The UEFA Champions League is Europe's premier club competition. African players have excelled in this tournament, with Mohamed Salah, Sadio Mané, and Riyad Mahrez all winning the trophy.",
        'african cup': "The Africa Cup of Nations (AFCON) is the main international football competition in Africa. It's organized by the Confederation of African Football (CAF) and is held every two years. The most successful country is Egypt with 7 titles.",
        'transfers': "The football transfer market involves the buying and selling of players between clubs. TacticSense provides risk intelligence for African football transfers to ensure safer, more informed decisions.",
        'salary': "Football player salaries vary widely depending on the league, club, and player profile. In African leagues, salaries are generally lower than in European leagues but are steadily increasing as the commercial aspect of the game grows.",
        'agent': "Football agents represent players in contract negotiations and transfers. TacticSense provides verification services for agents operating in the African football market.",
        'fifa': "FIFA (Fédération Internationale de Football Association) is the international governing body of football. CAF (Confederation of African Football) is the African football governing body under FIFA.",
        'rules': "Football is played with two teams of 11 players each. The basic rules include no handling the ball (except for goalkeepers in their area), scoring by getting the ball into the opponent's goal, and matches lasting 90 minutes divided into two halves.",
        'leagues': "The major African football leagues include the Egyptian Premier League, South African Premier Soccer League, Moroccan Botola, Tunisian Ligue Professionnelle 1, and the Nigerian Professional Football League.",
        'stadium': "Some of the largest football stadiums in Africa include Cairo International Stadium (Egypt), FNB Stadium (South Africa), and Stade Mohamed V (Morocco).",
    }
    
    query_lower = query.lower()
    for keyword, info in football_knowledge.items():
        if keyword in query_lower:
            return info
            
    # For queries about specific African countries and football
    african_countries = ['nigeria', 'egypt', 'south africa', 'ghana', 'senegal', 'cameroon', 'ivory coast', 
                         'morocco', 'tunisia', 'algeria', 'kenya', 'tanzania', 'uganda', 'ethiopia', 'zimbabwe']
    
    for country in african_countries:
        if country in query_lower:
            return f"{country.title()} has made significant contributions to African football. TacticSense tracks players, clubs, and stakeholders from {country.title()} to provide comprehensive risk intelligence and verification services."
    
    return None

def get_tacticsense_info(query):
    """Provide information about TacticSense platform features"""
    platform_info = {
        'risk': "TacticSense provides comprehensive risk assessment for football stakeholders in Africa. Our risk intelligence covers player transfers, club finances, agent reputation, and sponsor reliability.",
        'feature': "TacticSense features include stakeholder verification, risk assessment, transfer intelligence, sponsor matching, and detailed profiles of all football stakeholders across Africa.",
        'verify': "TacticSense verification services ensure that all stakeholders in the African football ecosystem are legitimate and trustworthy. We verify identities, contracts, credentials, and track records.",
        'how to': "Using TacticSense is simple: create an account, search for the stakeholder you want to investigate, view their verified profile with risk assessment, and make informed decisions based on our intelligence.",
        'service': "TacticSense services include stakeholder verification, risk assessment, transfer market intelligence, sponsor matching, and contract validation for the African football market.",
        'data': "TacticSense maintains comprehensive databases of players, clubs, agents, sponsors, and service providers across Africa. Our data is regularly updated and verified for accuracy.",
        'benefit': "The key benefits of TacticSense include reduced risk in football transactions, verified stakeholder information, better decision-making for clubs and sponsors, and a more transparent African football ecosystem.",
        'pricing': "TacticSense offers flexible subscription plans for different user types. Please contact our sales team for detailed pricing information based on your specific needs.",
        'start': "To get started with TacticSense, create an account on our platform, choose your subscription plan, and begin accessing our verified football stakeholder data and risk intelligence.",
        'contact': "You can contact TacticSense support through the 'Contact Us' section on our website or email support@tacticsense.com for any inquiries or assistance.",
    }
    
    query_lower = query.lower()
    for keyword, info in platform_info.items():
        if keyword in query_lower:
            return info
            
    return None
@app.route('/api/chatbot', methods=['POST'])
def process_query():
    data = request.json
    if 'query' not in data:
        return jsonify({'error': 'No query provided'}), 400
    
    query = data['query']
    user_id = data.get('user_id', 'default_user')  # Use IP or session ID in production
    
    # Get or create user context
    if user_id not in user_contexts:
        user_contexts[user_id] = get_default_context()
    
    context = user_contexts[user_id]
    print(f"Processing query: {query} (Context: {context})")
    
    # Handle follow-up questions
    if is_followup_query(query) and context['last_results'] is not None:
        return handle_followup_query(query, context)
    
    # Handle website info questions
    if is_website_info_query(query):
        return jsonify({
            'message': "TacticSense is a football intelligence platform focused on African football. It provides verified information about players, clubs, agents, sponsors, and service providers in the football industry across Africa. The platform helps users make informed decisions by offering risk analysis and stakeholder verification services for football professionals, clubs, and organizations."
        })

    # Handle African football questions
    if is_africa_football_query(query):
        return jsonify({
            'message': "African football has been growing rapidly in recent years, with more players from the continent entering top European leagues. Countries like Senegal, Nigeria, Egypt, Morocco, and Ghana have produced world-class talents. The Africa Cup of Nations (AFCON) is the major continental competition. TacticSense provides comprehensive data and risk intelligence about football stakeholders across Africa to support informed decision-making in player transfers, sponsorships, and club operations."
        })
    
    # Special handling for player age queries
    if 'player' in query.lower() and 'under' in query.lower():
        age_match = re.search(r'under (\d+)', query.lower())
        if age_match:
            try:
                age_limit = int(age_match.group(1))
                if 'players' in datasets:
                    players_df = datasets['players']
                    if 'age' in players_df.columns:
                        matching_players = players_df[players_df['age'] < age_limit]
                        result_count = len(matching_players)
                        
                        # Clean the dataframe for JSON
                        clean_df = clean_dataframe_for_json(matching_players.head(20))
                        
                        # Store for context
                        context['last_query'] = query
                        context['last_entity_type'] = 'players'
                        context['last_result_count'] = result_count
                        context['last_results'] = matching_players
                        
                        return jsonify({
                            'message': f"Here are some of the {result_count} players under {age_limit}:",
                            'result_data': clean_df.to_dict(orient='records')
                        })
            except Exception as e:
                print(f"Error processing age query: {e}")
                traceback.print_exc()
                return jsonify({'message': f"I found players under {age_limit} but had trouble formatting the data."})
        
    try:
        # Process the query with the chatbot
        response = chatbot.process_query(query)
        print(f"Raw response: {response}")
        
        # Special handling for age queries
        if query.lower().startswith('how many') and 'players' in query.lower():
            if 'under' in query.lower() or 'over' in query.lower() or 'age' in query.lower():
                try:
                    if 'players' in datasets:
                        players_df = datasets['players']
                        if 'age' in players_df.columns:
                            # Extract age number from query
                            age_match = re.search(r'under (\d+)', query.lower())
                            if age_match:
                                age_limit = int(age_match.group(1))
                                matching_players = players_df[players_df['age'] < age_limit]
                                result_count = len(matching_players)
                                
                                # Store in context for follow-up questions
                                context['last_query'] = query
                                context['last_entity_type'] = 'players'
                                context['last_result_count'] = result_count
                                context['last_results'] = matching_players
                                
                                return jsonify({
                                    'message': f"There are {result_count} players under {age_limit} in the database.",
                                    'count': result_count
                                })
                            
                            # Handle "over X" queries
                            age_match = re.search(r'over (\d+)', query.lower())
                            if age_match:
                                age_limit = int(age_match.group(1))
                                matching_players = players_df[players_df['age'] > age_limit]
                                result_count = len(matching_players)
                                
                                # Store in context for follow-up questions
                                context['last_query'] = query
                                context['last_entity_type'] = 'players'
                                context['last_result_count'] = result_count
                                context['last_results'] = matching_players
                                
                                return jsonify({
                                    'message': f"There are {result_count} players over {age_limit} in the database.",
                                    'count': result_count
                                })
                except Exception as e:
                    print(f"Error in age processing: {e}")
        
        # Handle specific entity queries
        for entity_type in ['players', 'clubs', 'sponsors', 'agents']:
            if entity_type in query.lower() and ('list' in query.lower() or 'show' in query.lower() or 'all' in query.lower()):
                try:
                    if entity_type in datasets or (entity_type == 'agents' and 'player_agents' in datasets):
                        df = datasets.get(entity_type, datasets.get('player_agents'))
                        
                        # Clean the dataframe for JSON
                        clean_df = clean_dataframe_for_json(df.head(20))
                        
                        # Update context for follow-up
                        context['last_query'] = query
                        context['last_entity_type'] = entity_type
                        context['last_result_count'] = len(df)
                        context['last_results'] = df
                        
                        return jsonify({
                            'message': f"I found {len(df)} {entity_type} in the database. Here are the first few:",
                            'result_data': clean_df.to_dict(orient='records')
                        })
                except Exception as e:
                    print(f"Error handling list query: {e}")
                    traceback.print_exc()

        # Store results in context if we have dataframe results
        if 'result_data' in response and hasattr(response['result_data'], 'to_dict'):
            try:
                # Detect entity type from the query
                entity_type = None
                for et in ['players', 'clubs', 'sponsors', 'agents', 'managers']:
                    if et in query.lower():
                        entity_type = et
                        break
                        
                # Clean the dataframe first
                clean_df = clean_dataframe_for_json(response['result_data'])
                
                # Store in context for follow-up
                context['last_query'] = query
                context['last_results'] = response['result_data']
                context['last_result_count'] = len(response['result_data'])
                context['last_entity_type'] = entity_type
                
                # Update the response with clean data
                response['result_data'] = clean_df.to_dict(orient='records')
            except Exception as conversion_error:
                print(f"Error converting dataframe to dict: {conversion_error}")
                traceback.print_exc()
                return jsonify({
                    'message': f"Found data but couldn't format it properly. Please try a different query."
                })
        
        # Update context with this response
        context['last_response'] = response.get('message', '')
        
        # Customize the response message if it's the default fallback
        default_message = "I'm not sure what you're looking for. Try asking about players, clubs, agents, sponsors, or equipment suppliers."
        if response.get('message') == default_message:
            response['message'] = "I don't have specific information on that. You can ask me about football in Africa, specific players, clubs, sponsors, or agents. Try questions like 'List all players', 'How many players are under 26?', or 'Tell me about football in Africa'."
        
        return jsonify(response)
        
    except Exception as e:
        print(f"Error processing query: {e}")
        print(traceback.format_exc())
        return jsonify({
            'message': f"I'm sorry, I encountered an error processing your request. Please try asking in a different way."
        })

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'message': 'Chatbot API is running'})

@app.route('/', methods=['GET'])
def index():
    return """
    <h1>TacticSense Chatbot API</h1>
    <p>API is running. Use POST /api/chatbot with a JSON body containing a "query" field.</p>
    <p>Example: <code>{"query": "Who is the sponsor of Manchester United?"}</code></p>
    """

if __name__ == '__main__':
    print("Starting Flask API server...")
    app.run(debug=True, port=5000)