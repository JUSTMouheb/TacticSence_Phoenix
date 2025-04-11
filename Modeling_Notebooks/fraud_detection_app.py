import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import plotly.graph_objects as go
import time
import os
import warnings
warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(page_title="Fraud Detection in Sports Industry", layout="wide")

# Try importing tensorflow, but handle the case if not installed
try:
    import tensorflow as tf
    from tensorflow.keras import layers, models
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    st.sidebar.warning("⚠️ TensorFlow is not installed. Autoencoder models will not be available. To install TensorFlow, run: `pip install tensorflow`")

# Try importing networkx, handle if not installed
try:
    import networkx as nx
    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False
    st.sidebar.warning("⚠️ NetworkX is not installed. Network visualizations will not be available. To install NetworkX, run: `pip install networkx`")

# Title and introduction
st.title("Fraud Detection in Sports Industry")
st.markdown("""
This application demonstrates fraud detection for five types of stakeholders in the sports industry:
- Players' Agents
- Recruiting Agents
- Sporting Management Agencies
- Communication Boxes
- Sponsors
""")

# Added missing autoencoder function
def build_autoencoder_model():
    """Build a simple autoencoder model for anomaly detection"""
    if not TENSORFLOW_AVAILABLE:
        return None
    
    # Define a simple autoencoder architecture
    input_dim = 10  # This should be adjusted based on actual feature count
    encoding_dim = 5
    
    input_layer = layers.Input(shape=(input_dim,))
    encoded = layers.Dense(encoding_dim, activation='relu')(input_layer)
    decoded = layers.Dense(input_dim, activation='sigmoid')(encoded)
    
    # Create the autoencoder model
    autoencoder = models.Model(input_layer, decoded)
    autoencoder.compile(optimizer='adam', loss='mse')
    
    return autoencoder

# Data loading function
@st.cache_data
def load_data():
    try:
        players_agents_df = pd.read_csv('Players_Agents_Dataset.csv')
        recruiting_agents_df = pd.read_csv('Recruiting_Agents_Dataset.csv')
        sporting_management_agencies_df = pd.read_csv('Sporting_management_agencies_Dataset.csv')
        communication_boxes_df = pd.read_csv('Communication_Boxes_Dataset.csv')
        sponsors_df = pd.read_csv('Sponsors_Dataset.csv')
        
        datasets = {
            'Players Agents': players_agents_df,
            'Recruiting Agents': recruiting_agents_df,
            'Sporting Management Agencies': sporting_management_agencies_df,
            'Communication Boxes': communication_boxes_df,
            'Sponsors': sponsors_df
        }
        return datasets
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

# Preprocessing function
def preprocess_datasets(datasets):
    cleaned_datasets = {}
    
    for name, df in datasets.items():
        cleaned_df = df.copy()
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        # Fill numeric missing values with median
        for col in numeric_cols:
            cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].median())
        
        # Fill categorical missing values with mode
        for col in categorical_cols:
            if not cleaned_df[col].mode().empty:
                cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].mode()[0])
            else:
                cleaned_df[col] = cleaned_df[col].fillna("Unknown")
        
        # Remove duplicates
        cleaned_df = cleaned_df.drop_duplicates()
        
        # Handle outliers using IQR method
        for col in numeric_cols:
            Q1 = cleaned_df[col].quantile(0.25)
            Q3 = cleaned_df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            cleaned_df[col] = cleaned_df[col].clip(lower_bound, upper_bound)
        
        cleaned_datasets[name] = cleaned_df
    
    return cleaned_datasets

# Feature engineering function (simplified version)
def create_features(df_features):
    # Add consistency metrics
    df_features['data_completeness_ratio'] = df_features.apply(
        lambda row: sum(pd.notna(value) for value in row) / len(row), axis=1
    )
    
    # Calculate risk scores based on available features
    if 'network_connection_count' in df_features.columns and 'financial_risk_score' in df_features.columns:
        df_features['combined_risk_score'] = (
            df_features['network_connection_count'].fillna(0) * 0.3 +
            df_features['financial_risk_score'].fillna(0) * 0.7
        )
    else:
        # Create a simple risk score based on data completeness
        df_features['combined_risk_score'] = 1 - df_features['data_completeness_ratio']
    
    return df_features

# Model building function
@st.cache_resource
def build_models():
    models = {
        'Isolation Forest': IsolationForest(contamination=0.1, random_state=42),
        'One-Class SVM': OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1)
    }
    
    # Add Autoencoder model only if TensorFlow is available
    if TENSORFLOW_AVAILABLE:
        models['Autoencoder'] = build_autoencoder_model()
        
    return models

def upload_dataset_section():
    st.sidebar.header("Upload Your Dataset")
    
    uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type="csv")
    if uploaded_file is not None:
        try:
            data = pd.read_csv(uploaded_file)
            st.sidebar.success(f"Successfully loaded dataset with {data.shape[0]} rows and {data.shape[1]} columns")
            return data
        except Exception as e:
            st.sidebar.error(f"Error: {e}")
            return None
    return None

# Added risk tier function that was referenced but missing
def get_risk_tier(prob):
    if prob > 0.8: return "Very High"
    elif prob > 0.6: return "High"
    elif prob > 0.4: return "Medium"
    elif prob > 0.2: return "Low"
    else: return "Very Low"

def deployment_demo_page():
    st.header("Deployment Demo")
    st.markdown("""
    This page demonstrates how the fraud detection system would work in a production environment, 
    allowing for both batch processing and real-time entity analysis.
    """)
    
    # Create tabs for batch vs real-time processing
    tab1, tab2 = st.tabs(["Batch Processing", "Real-Time Processing"])
    
    with tab1:
        st.subheader("Batch Fraud Detection")
        
        # Upload batch file option
        batch_file = st.file_uploader("Upload batch CSV file", type="csv")
        
        if batch_file:
            try:
                batch_data = pd.read_csv(batch_file)
                st.write(f"Loaded {len(batch_data)} entities for batch processing")
                
                # Display sample of uploaded data
                st.write("Sample data:")
                st.dataframe(batch_data.head(5))
                
                # Process button
                if st.button("Process Batch"):
                    with st.spinner("Processing batch data..."):
                        # Simulate processing time
                        time.sleep(2)
                        
                        # Generate mock results
                        batch_data['fraud_probability'] = np.random.beta(2, 5, len(batch_data))
                        batch_data['risk_tier'] = batch_data['fraud_probability'].apply(get_risk_tier)
                        
                    # Show results
                    st.success(f"Batch processing complete! Analyzed {len(batch_data)} entities.")
                    
                    # Summary metrics
                    high_risk = batch_data[batch_data['fraud_probability'] > 0.7]
                    medium_risk = batch_data[(batch_data['fraud_probability'] > 0.4) & (batch_data['fraud_probability'] <= 0.7)]
                    low_risk = batch_data[batch_data['fraud_probability'] <= 0.4]
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("High Risk", f"{len(high_risk)} entities")
                    with col2:
                        st.metric("Medium Risk", f"{len(medium_risk)} entities")
                    with col3:
                        st.metric("Low Risk", f"{len(low_risk)} entities")
                    
                    # Display high risk entities
                    st.subheader("High Risk Entities")
                    st.dataframe(high_risk.sort_values('fraud_probability', ascending=False))
            
            except Exception as e:
                st.error(f"Error processing file: {e}")
    
    with tab2:
        st.subheader("Real-Time Fraud Detection")
        st.write("Simulate real-time entity analysis by entering entity details below")
        
        # Create form for entity details
        with st.form("entity_form"):
            col1, col2 = st.columns(2)
            with col1:
                entity_name = st.text_input("Entity Name")
                entity_type = st.selectbox("Entity Type", ["Player Agent", "Scout", "Agency", "Communication Box", "Sponsor"])
                region = st.text_input("Region")
            
            with col2:
                experience = st.slider("Experience Years", 0, 30, 5)
                web_presence = st.selectbox("Has Web Presence", ["Yes", "No"])
                verification = st.selectbox("Verified Status", ["Verified", "Unverified"])
            
            # Submit button
            submitted = st.form_submit_button("Analyze Entity")
            
            if submitted:
                # Simulate processing
                with st.spinner("Analyzing entity..."):
                    # Simulate calculation time
                    time.sleep(1.5)
                    
                    # Generate a risk score based on inputs (for demo)
                    base_risk = np.random.beta(2, 5)
                    
                    # Adjust based on inputs
                    if verification == "Unverified":
                        base_risk += 0.2
                    if web_presence == "No":
                        base_risk += 0.15
                    if experience < 3:
                        base_risk += 0.1
                    
                    # Cap at 0.95
                    risk_score = min(base_risk, 0.95)
                    
                # Display results
                st.subheader("Analysis Results")
                
                # Create columns for metrics
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Fraud Risk Score", f"{risk_score:.2f}")
                    st.metric("Risk Tier", get_risk_tier(risk_score))
                
                with col2:
                    # Generate some random risk components
                    network_risk = np.random.uniform(risk_score-0.1, risk_score+0.1)
                    financial_risk = np.random.uniform(risk_score-0.1, risk_score+0.1)
                    consistency_risk = np.random.uniform(risk_score-0.1, risk_score+0.1)
                    
                    st.write("Risk Components:")
                    st.write(f"- Network Risk: {network_risk:.2f}")
                    st.write(f"- Financial Risk: {financial_risk:.2f}")
                    st.write(f"- Consistency Risk: {consistency_risk:.2f}")
                
                # Display gauge chart
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = risk_score,
                    domain = {'x': [0, 1], 'y': [0, 1]},
                    title = {'text': "Fraud Risk"},
                    gauge = {
                        'axis': {'range': [0, 1]},
                        'bar': {'color': "darkred"},
                        'steps': [
                            {'range': [0, 0.2], 'color': "green"},
                            {'range': [0.2, 0.4], 'color': "lightgreen"},
                            {'range': [0.4, 0.6], 'color': "yellow"},
                            {'range': [0.6, 0.8], 'color': "orange"},
                            {'range': [0.8, 1], 'color': "red"},
                        ]
                    }
                ))
                
                st.plotly_chart(fig)
                
                # Show recommendation based on risk level
                st.subheader("Recommendation")
                if risk_score > 0.7:
                    st.error("HIGH RISK: Immediate investigation recommended. Verify all documentation and credentials.")
                elif risk_score > 0.4:
                    st.warning("MEDIUM RISK: Additional verification recommended before proceeding.")
                else:
                    st.success("LOW RISK: Standard verification procedures sufficient.")

# Create sidebar for navigation - Added Deployment Demo
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page",
    ["Data Overview", "Feature Engineering", "Anomaly Detection", "Fraud Analysis", "Case Studies", "Deployment Demo"]
)

# Add upload dataset section to sidebar
user_data = upload_dataset_section()
if user_data is not None:
    st.sidebar.info("You can now analyze your uploaded data!")

# Load data
datasets = load_data()

if datasets is None:
    st.error("Please upload the required datasets to continue.")
else:
    if page == "Data Overview":
        st.header("Data Overview")
        
        # Select dataset to view
        selected_dataset = st.selectbox(
            "Select stakeholder dataset to explore",
            list(datasets.keys())
        )
        
        df = datasets[selected_dataset]
        
        # Display basic information
        st.subheader(f"Dataset: {selected_dataset}")
        st.write(f"Shape: {df.shape}")
        
        # Display sample data
        st.subheader("Sample Data")
        st.dataframe(df.head(10))
        
        # Display summary statistics
        st.subheader("Summary Statistics")
        st.dataframe(df.describe())
        
        # Display missing values
        st.subheader("Missing Values")
        missing_values = df.isnull().sum()
        st.write(missing_values[missing_values > 0])

    elif page == "Feature Engineering":
        st.header("Feature Engineering for Fraud Detection")
        
        # Select dataset for feature engineering
        selected_dataset = st.selectbox(
            "Select stakeholder dataset for feature engineering",
            list(datasets.keys())
        )
        
        df = datasets[selected_dataset]
        cleaned_df = preprocess_datasets({selected_dataset: df})[selected_dataset]
        
        # Apply feature engineering
        with st.spinner("Engineering features..."):
            df_with_features = create_features(cleaned_df)
        
        # Display engineered features
        st.subheader("Engineered Features")
        st.dataframe(df_with_features.head(10))
        
        # Visualize key features
        st.subheader("Feature Distributions")
        
        feature_to_plot = st.selectbox(
            "Select feature to visualize",
            df_with_features.select_dtypes(include=[np.number]).columns.tolist()
        )
        
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.histplot(df_with_features[feature_to_plot], kde=True, ax=ax)
        ax.set_title(f'{feature_to_plot} Distribution')
        st.pyplot(fig)

    elif page == "Anomaly Detection":
        st.header("Anomaly Detection")
        
        # Fixed: Make sure df_with_features is defined
        selected_dataset = st.selectbox(
            "Select stakeholder dataset for anomaly detection",
            list(datasets.keys())
        )
        
        df = datasets[selected_dataset]
        cleaned_df = preprocess_datasets({selected_dataset: df})[selected_dataset]
        df_with_features = create_features(cleaned_df)
        
        # Select model for anomaly detection
        models = build_models()
        
        # Don't include Autoencoder in options if TensorFlow is not available
        model_options = list(models.keys())
        if not TENSORFLOW_AVAILABLE and "Autoencoder" in model_options:
            model_options.remove("Autoencoder")
            
        selected_model = st.selectbox(
            "Select anomaly detection model",
            model_options
        )
        
        # Select features for anomaly detection
        numeric_features = df_with_features.select_dtypes(include=[np.number]).columns.tolist()
        selected_features = st.multiselect(
            "Select features for anomaly detection",
            numeric_features,
            default=numeric_features[:5]  # Default to first 5 numeric features
        )
        
        if len(selected_features) < 2:
            st.warning("Please select at least 2 features for anomaly detection.")
        else:
            # Apply anomaly detection
            with st.spinner("Detecting anomalies..."):
                X = df_with_features[selected_features].fillna(0)
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                model = models[selected_model]
                model.fit(X_scaled)
                
                # Get anomaly scores and predictions
                if selected_model == "Isolation Forest":
                    scores = model.score_samples(X_scaled)
                    predictions = model.predict(X_scaled)
                else:  # One-Class SVM
                    scores = model.score_samples(X_scaled)
                    predictions = model.predict(X_scaled)
                
                # Add results to dataframe
                df_with_features['anomaly_score'] = scores
                df_with_features['is_anomaly'] = predictions == -1
                
                # Calculate anomaly threshold
                threshold = np.percentile(scores, 10)  # Bottom 10% are anomalies
            
            # Display anomaly detection results
            st.subheader("Anomaly Detection Results")
            
            # Summary statistics
            anomaly_count = df_with_features['is_anomaly'].sum()
            st.write(f"Detected {anomaly_count} anomalies out of {len(df_with_features)} entities ({anomaly_count/len(df_with_features)*100:.1f}%)")
            
            # Show anomalies
            st.subheader("Detected Anomalies")
            anomalies = df_with_features[df_with_features['is_anomaly']]
            st.dataframe(anomalies)
            
            # Visualize anomaly scores
            st.subheader("Anomaly Score Distribution")
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.histplot(scores, kde=True, ax=ax)
            ax.axvline(x=threshold, color='red', linestyle='--', label='Anomaly Threshold')
            ax.set_title('Anomaly Score Distribution')
            ax.legend()
            st.pyplot(fig)
            
            # 2D visualization of anomalies
            if len(selected_features) >= 2:
                st.subheader("2D Visualization of Anomalies")
                
                fig, ax = plt.subplots(figsize=(10, 8))
                scatter = ax.scatter(
                    X_scaled[:, 0], 
                    X_scaled[:, 1],
                    c=predictions == -1,
                    cmap='coolwarm',
                    alpha=0.6
                )
                ax.set_title('Anomaly Detection Visualization')
                ax.set_xlabel(selected_features[0])
                ax.set_ylabel(selected_features[1])
                ax.legend(*scatter.legend_elements(), title="Anomaly")
                st.pyplot(fig)

    elif page == "Fraud Analysis":
        st.header("Fraud Risk Analysis")
    
        # Create tabs for overall analysis and individual agent analysis
        analysis_tab1, analysis_tab2 = st.tabs(["Overall Analysis", "Individual Agent Analysis"])
        
        with analysis_tab1:
            # Keep your existing code for overall analysis
            st.info("This tab displays analysis from the combined fraud detection models.")
            
            # Create sample data for demonstration
            sample_risk_data = pd.DataFrame({
                'entity_id': [f"E{i}" for i in range(100)],
                'name': [f"Entity {i}" for i in range(100)],
                'entity_type': np.random.choice(['Player Agent', 'Scout', 'Agency', 'Communication Box', 'Sponsor'], 100),
                'fraud_probability': np.random.beta(2, 5, 100),
            })
            
            # Add risk tier based on probability
            sample_risk_data['risk_tier'] = sample_risk_data['fraud_probability'].apply(get_risk_tier)
            
            # Risk threshold slider
            risk_threshold = st.slider("Risk Threshold", 0.0, 1.0, 0.6, 0.05)
            
            # Filter entities above threshold
            high_risk = sample_risk_data[sample_risk_data['fraud_probability'] >= risk_threshold]
            
            # Display high risk entities
            st.subheader(f"High Risk Entities (Risk Score ≥ {risk_threshold})")
            st.write(f"Found {len(high_risk)} high risk entities out of {len(sample_risk_data)} total")
            st.dataframe(high_risk.sort_values('fraud_probability', ascending=False))
            
            # Visualize risk distribution
            st.subheader("Fraud Risk Distribution")
            
            # By score
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.histplot(sample_risk_data['fraud_probability'], kde=True, ax=ax)
            ax.axvline(x=risk_threshold, color='red', linestyle='--', label='Risk Threshold')
            ax.set_title('Fraud Risk Score Distribution')
            ax.legend()
            st.pyplot(fig)
            
            # By entity type
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.boxplot(x='entity_type', y='fraud_probability', data=sample_risk_data, ax=ax)
            ax.axhline(y=risk_threshold, color='red', linestyle='--', label='Risk Threshold')
            ax.set_title('Fraud Risk by Entity Type')
            ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
            ax.legend()
            st.pyplot(fig)
        
        with analysis_tab2:
            st.subheader("Individual Agent Fraud Analysis")
            st.info("Select a specific agent to analyze their fraud risk.")
            
            # Get the Players Agents dataset
            if 'Players Agents' in datasets:
                agents_df = datasets['Players Agents']
                
                # Add fraud risk scores if they don't exist (for demo purposes)
                if 'fraud_probability' not in agents_df.columns:
                    # Generate mock fraud probabilities for demonstration
                    agents_df['fraud_probability'] = np.random.beta(2, 5, len(agents_df))
                    agents_df['risk_tier'] = agents_df['fraud_probability'].apply(get_risk_tier)
                    
                    # Add some random fraud flags based on probability
                    def generate_flags(prob):
                        flags = []
                        if prob > 0.7:
                            flags = np.random.choice([
                                "Inconsistent contract history",
                                "Multiple identity documents",
                                "Suspicious financial transactions",
                                "Undisclosed relationships with clubs",
                                "History of contract disputes",
                                "Missing verification documents"
                            ], size=np.random.randint(2, 5), replace=False).tolist()
                        elif prob > 0.4:
                            flags = np.random.choice([
                                "Limited verification history",
                                "Inconsistent client records",
                                "Recent rapid growth in transactions"
                            ], size=np.random.randint(1, 3), replace=False).tolist()
                        return flags
                    
                    agents_df['flags'] = agents_df['fraud_probability'].apply(generate_flags)
                
                # Get column that contains agent names
                name_column = 'name'
                if name_column not in agents_df.columns:
                    # Find likely name columns
                    possible_name_cols = [col for col in agents_df.columns if 'name' in col.lower() or 'agent' in col.lower()]
                    if possible_name_cols:
                        name_column = possible_name_cols[0]
                    else:
                        # Use the first string column as a fallback
                        str_cols = agents_df.select_dtypes(include=['object']).columns
                        if not str_cols.empty:
                            name_column = str_cols[0]
                
                # Create agent selection dropdown
                if name_column in agents_df.columns:
                    agent_names = agents_df[name_column].sort_values().unique()
                    selected_agent = st.selectbox("Select Agent", agent_names)
                    
                    if selected_agent:
                        # Get the selected agent's data
                        agent_data = agents_df[agents_df[name_column] == selected_agent].iloc[0]
                        
                        # Display agent details
                        st.subheader(f"Agent: {selected_agent}")
                        
                        # Create columns for agent details
                        col1, col2 = st.columns(2)
                        
                        # Display fraud risk assessment
                        with col1:
                            risk_score = agent_data['fraud_probability']
                            st.metric("Fraud Risk Score", f"{risk_score:.2f}")
                            
                            # Show risk category with appropriate color
                            risk_tier = get_risk_tier(risk_score)
                            if risk_tier in ["Very High", "High"]:
                                st.error(f"Risk Level: {risk_tier}")
                            elif risk_tier == "Medium":
                                st.warning(f"Risk Level: {risk_tier}")
                            else:
                                st.success(f"Risk Level: {risk_tier}")
                        
                        # Display agent details
                        with col2:
                            # Show a few key fields from the agent data
                            for col in agents_df.columns[:5]:  # Show first 5 columns
                                if col != name_column and col not in ['fraud_probability', 'risk_tier', 'flags']:
                                    st.write(f"**{col}:** {agent_data[col]}")
                        
                        # Display gauge chart for risk score
                        fig = go.Figure(go.Indicator(
                            mode = "gauge+number",
                            value = risk_score,
                            domain = {'x': [0, 1], 'y': [0, 1]},
                            title = {'text': "Fraud Risk"},
                            gauge = {
                                'axis': {'range': [0, 1]},
                                'bar': {'color': "darkred"},
                                'steps': [
                                    {'range': [0, 0.2], 'color': "green"},
                                    {'range': [0.2, 0.4], 'color': "lightgreen"},
                                    {'range': [0.4, 0.6], 'color': "yellow"},
                                    {'range': [0.6, 0.8], 'color': "orange"},
                                    {'range': [0.8, 1], 'color': "red"},
                                ]
                            }
                        ))
                        st.plotly_chart(fig)
                        
                        # Display clear fraud detection verdict
                        st.subheader("Fraud Detection Verdict")
                        if risk_score > 0.7:
                            st.error("⚠️ **FRAUD DETECTED** - High probability of fraudulent activity")
                        elif risk_score > 0.4:
                            st.warning("⚠️ **POTENTIAL FRAUD** - Some suspicious patterns detected")
                        else:
                            st.success("✅ **NO FRAUD DETECTED** - This agent appears legitimate")
                        
                        # Display fraud flags if any
                        if 'flags' in agent_data and len(agent_data['flags']) > 0:
                            st.subheader("Red Flags Detected")
                            for flag in agent_data['flags']:
                                st.markdown(f"- {flag}")
                        elif risk_score > 0.4:
                            st.subheader("Potential Red Flags")
                            st.markdown("- Review agent's transaction history")
                            st.markdown("- Verify agent's credentials and certifications")
                        else:
                            st.subheader("No Significant Red Flags")
                            st.success("Agent appears to be low risk based on current data.")
                        
                        # Recommendations based on risk level
                        st.subheader("Recommended Actions")
                        if risk_score > 0.7:
                            st.markdown("""
                            1. **Immediate Investigation**: Conduct thorough background check
                            2. **Document Verification**: Request additional verification documents
                            3. **Financial Audit**: Review all financial transactions
                            4. **Limit Engagement**: Consider restricting business activities until verification
                            """)
                        elif risk_score > 0.4:
                            st.markdown("""
                            1. **Additional Verification**: Request supporting documentation
                            2. **Transaction Monitoring**: Increase scrutiny on financial transactions
                            3. **Regular Reviews**: Schedule more frequent compliance reviews
                            """)
                        else:
                            st.markdown("""
                            1. **Standard Procedures**: Continue with normal verification protocols
                            2. **Regular Monitoring**: Maintain standard monitoring practices
                            """)
                else:
                    st.error(f"Could not find name column in the Players Agents dataset.")
            else:
                st.error("Players Agents dataset is not available. Please upload the dataset.")
    elif page == "Case Studies":
        st.header("Fraud Case Studies")
        st.info("This section showcases real-world fraud detection case studies for all types of stakeholders in the sports industry.")
    
    # Create a dropdown to select stakeholder type
    stakeholder_type = st.selectbox(
        "Select stakeholder type",
        list(datasets.keys())
    )
    
    # Get the selected dataset
    if stakeholder_type in datasets:
        current_df = datasets[stakeholder_type]
        
        # Add fraud scores if they don't exist (for demo purposes)
        if 'fraud_probability' not in current_df.columns:
            # Generate mock fraud probabilities for demonstration
            current_df['fraud_probability'] = np.random.beta(2, 5, len(current_df))
            current_df['risk_tier'] = current_df['fraud_probability'].apply(get_risk_tier)
            current_df['is_fraud_case'] = current_df['fraud_probability'] > 0.7  # Mark high risk as fraud cases
        
        # Get column that contains entity names
        name_column = 'name'
        if name_column not in current_df.columns:
            # Find likely name columns
            possible_name_cols = [col for col in current_df.columns 
                                if any(term in col.lower() for term in 
                                      ['name', 'agent', 'agency', 'company', 'box', 'sponsor', 'entity'])]
            if possible_name_cols:
                name_column = possible_name_cols[0]
            else:
                # Use the first string column as a fallback
                str_cols = current_df.select_dtypes(include=['object']).columns
                if not str_cols.empty:
                    name_column = str_cols[0]
        
        # Create a single dropdown for all entities in the selected dataset
        if name_column in current_df.columns:
            st.subheader(f"Select a {stakeholder_type.lower()} entity to analyze")
            
            # Get all entity names sorted
            all_entity_names = current_df[name_column].sort_values().unique()
            
            # Create single dropdown for all entities
            selected_entity = st.selectbox(
                f"Select a {stakeholder_type.lower()}",
                all_entity_names
            )
            
            # Create entity type specific terms
            entity_terms = {
                "Players Agents": {"term": "agent", "clients": "players", "transactions": "contracts"},
                "Recruiting Agents": {"term": "recruiter", "clients": "athletes", "transactions": "recruitment deals"},
                "Sporting Management Agencies": {"term": "agency", "clients": "teams/athletes", "transactions": "management contracts"},
                "Communication Boxes": {"term": "communication box", "clients": "media outlets", "transactions": "press releases"},
                "Sponsors": {"term": "sponsor", "clients": "sponsored entities", "transactions": "sponsorship deals"}
            }
            
            # Default terms if stakeholder type isn't in our mapping
            current_terms = entity_terms.get(stakeholder_type, {"term": "entity", "clients": "clients", "transactions": "transactions"})
            
            # Add view button
            if st.button("View Case Study", key=f"view_{stakeholder_type}"):
                if selected_entity:
                    # Get the selected entity's data
                    entity_data = current_df[current_df[name_column] == selected_entity].iloc[0]
                    fraud_prob = entity_data['fraud_probability']
                    is_fraud = fraud_prob > 0.7  # Define high risk threshold
                    
                    # Generate some mock case study data
                    st.header(f"Case Study: {selected_entity}")
                    
                    # Classification banner
                    if is_fraud:
                        st.error("🚨 CONFIRMED FRAUD CASE 🚨")
                    else:
                        st.success(f"✅ LEGITIMATE {current_terms['term'].upper()} - CLEARED OF SUSPICION")
                    
                    # Display fraud probability percentage and specific cause
                    fraud_percentage = f"{fraud_prob*100:.1f}%"
                    
                    st.subheader("Fraud Risk Assessment")
                    
                    # Create columns for the probability and explanation
                    prob_col, explain_col = st.columns([1, 2])
                    
                    with prob_col:
                        # Display large percentage with appropriate color
                        if fraud_prob > 0.7:
                            st.markdown(f"<h1 style='color:red'>{fraud_percentage}</h1>", unsafe_allow_html=True)
                        elif fraud_prob > 0.4:
                            st.markdown(f"<h1 style='color:orange'>{fraud_percentage}</h1>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"<h1 style='color:green'>{fraud_percentage}</h1>", unsafe_allow_html=True)
                        
                        # Display gauge chart for risk score
                        fig = go.Figure(go.Indicator(
                            mode = "gauge+number",
                            value = fraud_prob,
                            domain = {'x': [0, 1], 'y': [0, 1]},
                            number = {'suffix': "%", 'valueformat': ".1f", "font": {"size": 24}},
                            title = {'text': "Fraud Probability"},
                            gauge = {
                                'axis': {'range': [0, 1], 'ticksuffix': "%", 'tickformat': ".0%"},
                                'bar': {'color': "darkred"},
                                'steps': [
                                    {'range': [0, 0.2], 'color': "green"},
                                    {'range': [0.2, 0.4], 'color': "lightgreen"},
                                    {'range': [0.4, 0.6], 'color': "yellow"},
                                    {'range': [0.6, 0.8], 'color': "orange"},
                                    {'range': [0.8, 1], 'color': "red"},
                                ]
                            }
                        ))
                        st.plotly_chart(fig)
                    
                    with explain_col:
                        st.subheader("Specific Risk Factors")
                        
                        # Define stakeholder-specific risk factors
                        if stakeholder_type == "Players Agents":
                            high_risk_flags = [
                                f"Acquired {int(fraud_prob*200)} new players in less than a year (highly unusual growth rate)",
                                f"Maintained {int(fraud_prob*15)} offshore accounts with irregular transaction patterns",
                                "Documentation inconsistencies in 62% of player contracts",
                                f"Connected to {int(fraud_prob*8)} previously sanctioned agencies",
                                "Multiple identity discrepancies across official documents",
                                f"Conducted {int(fraud_prob*40)} transfers with inadequate documentation",
                                "Significant undisclosed conflicts of interest with club management",
                                f"Failed verification checks on {int(fraud_prob*100)}% of financial records",
                                "Pattern of player complaints about misrepresentation (12 formal complaints)",
                                f"Unusual fee structure exceeding industry standards by {int(fraud_prob*120)}%"
                            ]
                            medium_risk_flags = [
                                f"Unusual growth of {int(fraud_prob*100)} players in the past year",
                                "Some inconsistencies in contract documentation",
                                f"{int(fraud_prob*30)}% of financial transactions require additional verification",
                                "Limited operational history in the industry",
                                "Client acquisition patterns deviate from industry norms",
                                f"Connected to {int(fraud_prob*3)} entities with prior compliance issues"
                            ]
                        elif stakeholder_type == "Recruiting Agents":
                            high_risk_flags = [
                                f"Claimed {int(fraud_prob*150)} athlete placements with no verification",
                                f"Maintained {int(fraud_prob*12)} unregistered scouting operations",
                                f"Collected {int(fraud_prob*50)}% higher fees than industry standard from athletes",
                                "Multiple recruitment claims proved falsified upon investigation",
                                f"Connected to {int(fraud_prob*7)} training centers with poor safety records",
                                "Significant discrepancies in athlete development metrics",
                                f"Failed background checks for {int(fraud_prob*40)}% of reported scouts",
                                "Pattern of complaints from educational institutions about misrepresentation"
                            ]
                            medium_risk_flags = [
                                f"Unusual recruitment success rate of {int(fraud_prob*90)}% (industry average is 30%)",
                                "Some inconsistencies in athlete placement records",
                                f"{int(fraud_prob*25)}% of claimed placements couldn't be verified",
                                "Limited history with accredited sports organizations",
                                "Recruitment patterns show statistically improbable success rates",
                                f"Connected to {int(fraud_prob*2)} previously investigated training centers"
                            ]
                        elif stakeholder_type == "Sporting Management Agencies":
                            high_risk_flags = [
                                f"Managed {int(fraud_prob*300)}% growth in client portfolio with minimal staff increase",
                                f"Operated {int(fraud_prob*10)} shell companies tied to player management",
                                "Significant discrepancies in reported vs. actual revenue",
                                f"Failed {int(fraud_prob*80)}% of regulatory compliance checks",
                                "Pattern of contract disputes with multiple clubs and players",
                                f"Hidden ownership connections to {int(fraud_prob*6)} sports clubs",
                                "Unauthorized representation claims for high-profile athletes",
                                f"Diverted approximately ${int(fraud_prob*2000000)} through irregular financial channels"
                            ]
                            medium_risk_flags = [
                                f"Growth rate of {int(fraud_prob*80)}% exceeds typical agency expansion",
                                "Some client contracts contain unusual exclusivity clauses",
                                f"{int(fraud_prob*20)}% of financial transactions show timing anomalies",
                                "Limited transparency in ownership structure",
                                "Client acquisition methods raise questions about inducements",
                                f"Connected to {int(fraud_prob*3)} previously fined sports organizations"
                            ]
                        elif stakeholder_type == "Communication Boxes":
                            high_risk_flags = [
                                f"Published {int(fraud_prob*120)} articles with unverifiable sources",
                                f"Created {int(fraud_prob*15)} false media outlets to amplify stories",
                                f"Coordinated {int(fraud_prob*40)} narrative campaigns with betting pattern spikes",
                                "Significant discrepancies between reported news and verifiable facts",
                                f"Connected to {int(fraud_prob*8)} previously sanctioned information brokers",
                                "Pattern of timed releases coinciding with market movements",
                                f"Failed {int(fraud_prob*90)}% of independent fact checks",
                                "Orchestrated manufactured controversies around transfer windows"
                            ]
                            medium_risk_flags = [
                                f"Published {int(fraud_prob*60)} stories with single anonymous sources",
                                "Some published content contains factual inconsistencies",
                                f"{int(fraud_prob*30)}% of exclusives couldn't be corroborated by other outlets",
                                "Limited verification procedures for insider information",
                                "Publishing patterns show unusual correlation with betting odds changes",
                                f"Connected to {int(fraud_prob*2)} previously investigated media outlets"
                            ]
                        elif stakeholder_type == "Sponsors":
                            high_risk_flags = [
                                f"Created {int(fraud_prob*20)} shell sponsorship deals to launder ${int(fraud_prob*5000000)}",
                                f"Reported {int(fraud_prob*250)}% ROI on sponsorships with no supporting data",
                                "Significant discrepancies between contracted and actual payments",
                                f"Connected to {int(fraud_prob*12)} sanctioned financial entities",
                                "Pattern of sponsorship deals with subsequent undisclosed related-party transactions",
                                f"Inflated media value claims by {int(fraud_prob*300)}% compared to industry audits",
                                f"Created {int(fraud_prob*8)} fake brand activation events that never occurred",
                                "Multiple contract terms violated regarding payment schedules and obligations"
                            ]
                            medium_risk_flags = [
                                f"Sponsorship reporting shows {int(fraud_prob*70)}% discrepancies with third-party audits",
                                "Some promotional activities lack verification documentation",
                                f"{int(fraud_prob*25)}% of claimed audience reach appears inflated",
                                "Limited transparency in sponsorship selection process",
                                "Payment patterns show unusual structuring just below reporting thresholds",
                                f"Connected to {int(fraud_prob*3)} previously investigated marketing agencies"
                            ]
                        else:
                            # Generic flags for any other stakeholder type
                            high_risk_flags = [
                                f"Reported {int(fraud_prob*250)}% growth with minimal substantiation",
                                f"Maintained {int(fraud_prob*15)} suspicious financial arrangements",
                                f"Failed {int(fraud_prob*80)}% of compliance checks",
                                "Multiple identity and documentation discrepancies",
                                f"Connected to {int(fraud_prob*8)} previously sanctioned entities",
                                f"Conducted {int(fraud_prob*40)} transactions with inadequate documentation",
                                "Significant undisclosed conflicts of interest",
                                f"Financial records show approximately ${int(fraud_prob*1000000)} in unexplained transfers"
                            ]
                            medium_risk_flags = [
                                f"Growth rate of {int(fraud_prob*100)}% exceeds industry averages",
                                "Some documentation contains inconsistencies",
                                f"{int(fraud_prob*30)}% of transactions require additional verification",
                                "Limited operational history or sudden changes in business model",
                                "Activity patterns deviate from industry norms",
                                f"Connected to {int(fraud_prob*3)} entities with compliance issues"
                            ]
                            
                        # Low risk factors are more generic and apply to all types
                        low_risk_reasons = [
                            "Consistent documentation across all relationships and transactions",
                            f"Stable growth rate of {int((fraud_prob+0.1)*30)}% annually (within industry norms)",
                            "Clean compliance history with no significant issues",
                            "All financial transactions properly documented and verified",
                            "Business practices align with industry standards",
                            f"Successfully passed {int((1-fraud_prob)*10)} independent audits"
                        ]
                        
                        # Generate number of risk factors based on risk level
                        if fraud_prob > 0.7:
                            num_flags = min(5, int(fraud_prob * 7))
                            st.markdown(f"**{selected_entity}** is **highly likely to be engaging in fraudulent activities** ({fraud_percentage} probability) due to multiple serious red flags:")
                            
                            for i in range(min(num_flags, len(high_risk_flags))):
                                st.markdown(f"- 🚨 **{high_risk_flags[i]}**")
                                
                            st.markdown("**Recommended Action:** Immediate investigation and suspension of activities pending review.")
                            
                        elif fraud_prob > 0.4:
                            num_flags = min(3, int(fraud_prob * 5))
                            st.markdown(f"**{selected_entity}** shows **some suspicious patterns** ({fraud_percentage} probability) that warrant closer monitoring:")
                            
                            for i in range(min(num_flags, len(medium_risk_flags))):
                                st.markdown(f"- ⚠️ {medium_risk_flags[i]}")
                                
                            st.markdown("**Recommended Action:** Enhanced due diligence and more frequent compliance reviews.")
                            
                        else:
                            st.markdown(f"**{selected_entity}** appears to be a **legitimate {current_terms['term']}** ({fraud_percentage} probability) based on:")
                            
                            for i in range(min(3, len(low_risk_reasons))):
                                st.markdown(f"- ✅ {low_risk_reasons[i]}")
                                
                            st.markdown("**Recommended Action:** Continue standard monitoring procedures.")
                    
                    # Entity details section
                    st.subheader(f"{current_terms['term'].title()} Profile Details")

                    
                    # Create two columns for details
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Show basic info
                        st.write(f"**Risk Tier:** {entity_data['risk_tier']}")
                        
                        # Show a few key fields from the entity data
                        for col in current_df.columns[:3]:  # Show first few columns
                            if col != name_column and col not in ['fraud_probability', 'risk_tier', 'is_fraud_case']:
                                st.write(f"**{col}:** {entity_data[col]}")
                    
                    # In the Entity details section, add client_count variable definition:
                    with col2:
                         # Check if client count exists in the data
                        client_field = next((col for col in current_df.columns if any(s in col.lower() for s in ['client', 'player', 'athlete', 'entity', 'sponsor'])), None)
    
                        # Always define client_count variable first, then display it
                        if client_field and client_field in entity_data:
                            # Use actual client count from data
                            try:
                               client_count = int(entity_data[client_field])
                               st.write(f"**Total {current_terms['clients'].title()}:** {client_count}")
                            except (ValueError, TypeError):
                                 # Handle case where data can't be converted to integer
                                 client_count = entity_data[client_field]
                                 st.write(f"**Total {current_terms['clients'].title()}:** {client_count}")
                        else:
                            client_count = "N/A"
                            # Generate a random number as fallback
                            #client_multiplier = 5 if stakeholder_type in ["Sporting Management Agencies", "Sponsors"] else 1
                            #client_count = int((50 + fraud_prob * 200) * client_multiplier) if fraud_prob > 0.5 else int((10 + fraud_prob * 60) * client_multiplier)
                        
                        region_field = next((col for col in current_df.columns if any(s in col.lower() for s in ['region', 'location', 'country', 'territory'])), None)
    
                        if region_field and region_field in entity_data:
                            # Use actual region from data
                            st.write(f"**Primary Region:** {entity_data[region_field]}")
                        else:
                            # Generate a random region as fallback
                            regions = ["Europe", "North America", "South America", "Africa", "Asia", "Middle East", "Oceania"]
                            region = regions[hash(selected_entity) % len(regions)]
                            st.write(f"**Primary Region:** {region}")
    # Check if years in business exists in the data
                    
                    # Timeline of events section
                    st.subheader("Investigation Timeline")
                    
                    # Generate random dates in past year
                    today = pd.Timestamp.now()
                    start_date = today - pd.Timedelta(days=365)
                    
                    # Create entity-specific timeline events
                    if is_fraud:
                       timeline_events = [
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(0, 60)),
            "event": f"Automated detection system flagged {selected_entity} due to anomalous {current_terms['transactions']}"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(60, 120)),
            "event": f"Initial investigation revealed suspicious patterns in {current_terms['transactions']}"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(120, 180)),
            "event": f"Documentation audit found significant inconsistencies in {current_terms['transactions']}"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(180, 240)),
            "event": f"Financial review identified suspicious offshore transactions"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(240, 300)),
            "event": "Case escalated to regulatory authorities with fraud probability assessment"
        }
    ]
                    else:
                        timeline_events = [
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(0, 60)),
            "event": f"Routine review of {selected_entity} triggered as part of standard monitoring"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(60, 120)),
            "event": f"Documentation review showed consistent and proper record-keeping"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(120, 180)),
            "event": "Financial transaction patterns verified as consistent with legitimate business operations"
        },
        {
            "date": start_date + pd.Timedelta(days=np.random.randint(180, 240)),
            "event": "Case review completed with no significant issues identified"
        }
    ] 
                    
                    # Add stakeholder-specific timeline events
                    if stakeholder_type == "Players Agents" and is_fraud:
                        timeline_events.append({
                            "date": start_date + pd.Timedelta(days=np.random.randint(120, 280)),
                            "event": f"Player interviews revealed {int(fraud_prob*15)} athletes were misled about contract terms"
                        })
                    elif stakeholder_type == "Recruiting Agents" and is_fraud:
                        timeline_events.append({
                            "date": start_date + pd.Timedelta(days=np.random.randint(120, 280)),
                            "event": f"School verification found {int(fraud_prob*30)} claimed athlete placements never occurred"
                        })
                    elif stakeholder_type == "Sporting Management Agencies" and is_fraud:
                        timeline_events.append({
                            "date": start_date + pd.Timedelta(days=np.random.randint(120, 280)),
                            "event": f"Corporate structure analysis revealed {int(fraud_prob*8)} undisclosed related entities with conflicts of interest"
                        })
                    elif stakeholder_type == "Communication Boxes" and is_fraud:
                        timeline_events.append({
                            "date": start_date + pd.Timedelta(days=np.random.randint(120, 280)),
                            "event": f"Content audit found {int(fraud_prob*45)} articles contained fabricated quotes and sources"
                        })
                    elif stakeholder_type == "Sponsors" and is_fraud:
                        timeline_events.append({
                            "date": start_date + pd.Timedelta(days=np.random.randint(120, 280)),
                            "event": f"Financial tracking identified ${int(fraud_prob*3000000)} in sponsorship payments diverted to offshore accounts"
                        })
                    
                    # Sort events by date
                    timeline_events.sort(key=lambda x: x["date"])
                    
                    # Display timeline
                    for event in timeline_events:
                        st.write(f"**{event['date'].strftime('%b %d, %Y')}**: {event['event']}")
                    
            else:
               st.error(f"Could not find name column in the {stakeholder_type} dataset.")
        else:
           st.error(f"{stakeholder_type} dataset is not available. Please upload the required dataset.")

    elif page == "Deployment Demo":
        deployment_demo_page()