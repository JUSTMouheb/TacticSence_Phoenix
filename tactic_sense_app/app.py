import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import os
import chardet  # For automatic encoding detection

# Clear Streamlit Cache
st.cache_data.clear()

# Set Streamlit Page Configuration
st.set_page_config(page_title="TacticSense Data Validation", layout="wide")

# Title
st.title("📊 TacticSense Data Validation & Exploration")

# Sidebar Header
st.sidebar.header("📂 Select a Dataset")

# Define dataset paths
dataset_options = {
    "African Football Clubs": "data/African_Clubs_Complete.csv",
    "African Sponsorship Brands": "data/african_sponsorship_brands.csv",
    "Cleaned Dataset": "data/cleaned_ds.csv",
    "Communication Boxes Africa": "data/communication_boxes_africa_updated.csv",
    "Final Equipment Suppliers": "data/Final_equipment_suppliers_cleaned_utf8.csv",
    "Fitness Clubs": "data/Fitness_clubs (1).xlsx",
    "Output Enhanced": "data/output_enhanced.csv",
    "Player Agent": "data/player_agent.csv",
    "Player Data": "data/Player_DataFinal.csv",
    "Service Providers": "data/service_providers.csv",
    "Sponsors Dataset": "data/sponsors_dataset.csv",
    "Sports Management Agencies": "data/sports_management_agencies.csv",
    "Travel Agencies": "data/travel_agencies_dataset.csv",
}

# Dataset Selection
selected_dataset = st.sidebar.selectbox("Choose a dataset", list(dataset_options.keys()))

# Function to Detect Encoding
def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read(100000))  # Analyze 100,000 bytes
        return result["encoding"]

# Function to Load Dataset with Encoding Handling
@st.cache_data
def load_data(file_path):
    """Load dataset with encoding detection and handling."""
    
    # Check if file exists
    if not os.path.exists(file_path):
        st.error(f"❌ Error: File not found at {file_path}")
        return None
    
    # Detect encoding
    detected_encoding = detect_encoding(file_path)
    st.write(f"🔍 Detected Encoding: {detected_encoding}")

    encodings = [detected_encoding, "utf-8", "ISO-8859-1", "latin1", "windows-1252"]
    
    for enc in encodings:
        try:
            if file_path.endswith('.csv'):
                st.write(f"📂 Trying to load {file_path} using encoding: {enc}")
                df = pd.read_csv(file_path, encoding=enc, on_bad_lines="skip")  # Skip bad lines
                st.write(f"✅ Successfully loaded {file_path} with encoding: {enc}")
                return df
            elif file_path.endswith('.xlsx'):
                st.write(f"📂 Trying to load {file_path} (Excel format)")
                df = pd.read_excel(file_path)
                st.write(f"✅ Successfully loaded {file_path} (Excel format)")
                return df
        except UnicodeDecodeError:
            st.warning(f"⚠️ Encoding {enc} failed. Trying next...")
        except Exception as e:
            st.error(f"❌ Error loading {file_path} with encoding {enc}: {e}")

    st.error(f"❌ Could not load {file_path} with any encoding.")
    return None

# If a dataset is selected, load it
if selected_dataset:
    dataset_path = dataset_options[selected_dataset]
    
    if os.path.exists(dataset_path):
        df = load_data(dataset_path)
        
        if df is not None:
            st.success(f"✅ Loaded {selected_dataset} dataset!")

            # Display dataset preview
            st.write(f"### {selected_dataset} - Preview")
            st.dataframe(df.head())

            # Missing Values Analysis
            st.write("### 🔍 Missing Values Overview")
            missing_values = df.isnull().sum()
            st.dataframe(pd.DataFrame({"Column": df.columns, "Missing": missing_values}))

            # Duplicate Check
            duplicate_count = df.duplicated().sum()
            st.write(f"🚨 **Duplicate Rows Found:** {duplicate_count}")

            # Column Selection for Analysis
            selected_column = st.selectbox("📊 Select a column for visualization", df.columns)

            # **Histogram**
            st.write(f"### Distribution of {selected_column}")
            fig_hist = px.histogram(df, x=selected_column)
            st.plotly_chart(fig_hist)

            # **Bar Chart for Categorical Columns**
            if df[selected_column].dtype == "object":
                st.write(f"### Bar Chart of {selected_column}")

                # Fix column naming issue
                value_counts_df = df[selected_column].value_counts().reset_index()
                value_counts_df.columns = [selected_column, "count"]

                fig_bar = px.bar(value_counts_df, x=selected_column, y="count", 
                                 labels={selected_column: "Category", "count": "Count"})
                st.plotly_chart(fig_bar)

            # **Correlation Heatmap (Only for Numerical Columns)**
            if df.select_dtypes(include=['number']).shape[1] > 1:
                st.write("### Correlation Matrix")
                numeric_df = df.select_dtypes(include=['number'])  # Select only numeric columns
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.heatmap(numeric_df.corr(), annot=True, cmap="coolwarm", ax=ax)
                st.pyplot(fig)

            # **Boxplot for Outliers**
            if df[selected_column].dtype in ['int64', 'float64']:
                st.write(f"### Boxplot of {selected_column}")
                fig_box = px.box(df, y=selected_column)
                st.plotly_chart(fig_box)

            # **Pie Chart for Categorical Columns**
            if df[selected_column].dtype == "object":
                st.write(f"### Pie Chart of {selected_column}")
                fig_pie = px.pie(df, names=selected_column)
                st.plotly_chart(fig_pie)

            # **Scatter Plot for Numerical Columns**
            if df.select_dtypes(include=['number']).shape[1] > 1:
                num_columns = df.select_dtypes(include=['number']).columns.tolist()
                x_col = st.selectbox("Select X-axis for Scatter Plot", num_columns, index=0)
                y_col = st.selectbox("Select Y-axis for Scatter Plot", num_columns, index=1)

                st.write(f"### Scatter Plot: {x_col} vs {y_col}")
                fig_scatter = px.scatter(df, x=x_col, y=y_col, color=df[selected_column])
                st.plotly_chart(fig_scatter)

            # **Descriptive Statistics**
            st.write("### 📊 Statistical Summary")
            st.write(df.describe().T)

    else:
        st.error(f"❌ The file for {selected_dataset} could not be found at: {dataset_path}")
