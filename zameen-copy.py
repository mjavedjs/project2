#!/usr/bin/env python
# coding: utf-8

# zeapp.py - Complete Zameen Karachi Property Analysis

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

def fetchData(pages=20):
    """
    Scrape property data from Zameen.com
    """
    all_data = []

    for page in range(1, pages + 1):
        url = f"https://www.zameen.com/Homes/Karachi_Gulshan_e_Iqbal_Town-6858-{page}.html"
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "lxml")

        homes = soup.find_all('li', role="article")

        for home in homes:
            title_div = home.find("div", class_="d870ae17")
            location = title_div["title"] if title_div else None

            price_tag = home.find('span', {"aria-label": "Price"})
            price = price_tag.get_text(strip=True) if price_tag else None

            features_div = home.find("div", class_="e3fdfcd8")
            features = features_div.get_text(strip=True) if features_div else None

            last_updated_div = home.find("span", class_="a018d4bd")
            last_updated = last_updated_div.get_text(strip=True) if last_updated_div else None

            all_data.append({
                "Location": location,
                "Price": price,
                "Features": features,
                "Last Updated": last_updated
            })

    df = pd.DataFrame(all_data)
    print(df)
    df.to_csv("zameen_karachi_10pages.csv", index=False)
    print("Data saved to zameen_karachi_10pages.csv")
    return df

def clean_data(df):
    """
    Clean and process the scraped data
    """
    def price_to_number(price):
        if pd.isna(price):
            return None
        price = price.replace("PKR", "").strip()
        if "Crore" in price:
            return float(price.replace("Crore", "").strip()) * 10_000_000
        elif "Lakh" in price:
            return float(price.replace("Lakh", "").strip()) * 100_000
        else:
            try:
                return float(price)
            except:
                return None

    df["Price_numeric"] = df["Price"].apply(price_to_number)

    def extract_bed(features):
        if pd.isna(features):
            return None
        match = re.search(r'(\d+)\s*Bed', features)
        return int(match.group(1)) if match else None

    def extract_bath(features):
        if pd.isna(features):
            return None
        match = re.search(r'(\d+)\s*Bath', features)
        return int(match.group(1)) if match else None

    df["Bedrooms"] = df["Features"].apply(extract_bed)
    df["Bathrooms"] = df["Features"].apply(extract_bath)

    df = df.dropna(subset=["Price_numeric"])

    df["Bedrooms"] = pd.to_numeric(df["Bedrooms"], errors='coerce').fillna(0).astype(int)
    df["Bathrooms"] = pd.to_numeric(df["Bathrooms"], errors='coerce').fillna(0).astype(int)

    df_clean = df[["Location", "Price", "Price_numeric", "Bedrooms", "Bathrooms"]]

    df_clean.to_csv("zameen_karachi_10pages_clean.csv", index=False)
    print("Cleaned data saved to 'zameen_karachi_10pages_clean.csv'")
    
    print(df_clean.head(10))
    print(df_clean.info())
    return df_clean

def insert_to_database(df):
    """
    Insert data into SQL Server database
    """
    conn = pyodbc.connect(
        'Driver={SQL Server};'
        'Server=DESKTOP-RLMEU2F;'
        'Database=ZameenKarachi;'
        'Trusted_Connection=yes;'
    )

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO ZameenKarachi (Location,Price, Price_numeric, Bedrooms, Bathrooms)
    VALUES (?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["Location"],
            row["Price"],
            row["Price_numeric"],
            row["Bedrooms"],
            row["Bathrooms"]
        ))

    conn.commit()
    conn.close()

    print("✅ Data inserted successfully using pyodbc!")

def load_data():
    """
    Load data from SQL Server database for Streamlit app
    """
    try:
        conn = pyodbc.connect(
            'Driver={SQL Server};'
            'Server=DESKTOP-RLMEU2F;'
            'Database=ZameenKarachi;'
            'Trusted_Connection=yes;'
        )
        
        query = "SELECT * FROM ZameenKarachi"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def main():
    """
    Main function to run the complete pipeline
    """
    print("🚀 Starting Zameen Karachi Property Analysis...")
    
    # Step 1: Scrape data
    print("📊 Step 1: Scraping data from Zameen.com...")
    df_raw = fetchData(pages=20)
    
    # Step 2: Clean data
    print("🧹 Step 2: Cleaning and processing data...")
    df_clean = clean_data(df_raw)
    
    # Step 3: Insert to database
    print("💾 Step 3: Inserting data into database...")
    insert_to_database(df_clean)
    
    print("✅ All steps completed successfully!")
    
    # Step 4: Run Streamlit app
    print("🌐 Starting Streamlit web application...")
    run_streamlit_app()

def run_streamlit_app():
    """
    Run the Streamlit web application
    """
    # Set page configuration
    st.set_page_config(
        page_title="Zameen Karachi Analysis",
        page_icon="🏠",
        layout="wide"
    )

    # Title
    st.title("🏠 Zameen Karachi Property Analysis")
    st.markdown("Analyzing property trends in Karachi using Zameen data")

    # Load data
    df = load_data()

    if df is not None:
        # Display basic info
        st.sidebar.header("🔍 Filters")
        
        # Show dataset info
        st.subheader("📊 Dataset Overview")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Properties", len(df))
        
        with col2:
            st.metric("Average Price", f"PKR {df['Price_numeric'].mean():,.0f}")
        
        with col3:
            st.metric("Unique Locations", df['Location'].nunique())
        
        with col4:
            st.metric("Avg Bedrooms", f"{df['Bedrooms'].mean():.1f}")

        # Filter options
        all_locations = df['Location'].unique()
        selected_locations = st.sidebar.multiselect(
            "Select Locations:",
            options=all_locations,
            default=all_locations[:5] if len(all_locations) > 5 else all_locations
        )
        
        price_range = st.sidebar.slider(
            "Price Range (PKR):",
            min_value=int(df['Price_numeric'].min()),
            max_value=int(df['Price_numeric'].max()),
            value=(int(df['Price_numeric'].min()), int(df['Price_numeric'].max()))
        )
        
        # Apply filters
        if selected_locations:
            filtered_df = df[
                (df['Location'].isin(selected_locations)) &
                (df['Price_numeric'] >= price_range[0]) &
                (df['Price_numeric'] <= price_range[1])
            ]
        else:
            filtered_df = df[
                (df['Price_numeric'] >= price_range[0]) &
                (df['Price_numeric'] <= price_range[1])
            ]

        st.write(f"Showing {len(filtered_df)} properties after filtering")

        # ANALYSIS 1: Price Distribution by Location
        st.header("1. 📈 Price Distribution by Location")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Average Price by Location")
            avg_price_location = filtered_df.groupby('Location')['Price_numeric'].mean().sort_values(ascending=False).head(10)
            
            fig, ax = plt.subplots(figsize=(10, 6))
            avg_price_location.plot(kind='bar', ax=ax, color='skyblue')
            ax.set_title('Top 10 Locations by Average Price')
            ax.set_ylabel('Average Price (PKR)')
            ax.tick_params(axis='x', rotation=45)
            st.pyplot(fig)
        
        with col2:
            st.subheader("Price Distribution")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(filtered_df['Price_numeric'], bins=30, alpha=0.7, color='lightgreen', edgecolor='black')
            ax.set_title('Property Price Distribution')
            ax.set_xlabel('Price (PKR)')
            ax.set_ylabel('Number of Properties')
            st.pyplot(fig)

        # ANALYSIS 2: Bedroom Analysis
        st.header("2. 🛏️ Bedroom Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Properties by Bedroom Count")
            bedroom_count = filtered_df['Bedrooms'].value_counts().sort_index()
            
            fig, ax = plt.subplots(figsize=(10, 6))
            bedroom_count.plot(kind='bar', ax=ax, color='orange', alpha=0.7)
            ax.set_title('Number of Properties by Bedrooms')
            ax.set_xlabel('Number of Bedrooms')
            ax.set_ylabel('Count')
            st.pyplot(fig)
        
        with col2:
            st.subheader("Average Price by Bedrooms")
            avg_price_bedrooms = filtered_df.groupby('Bedrooms')['Price_numeric'].mean()
            
            fig, ax = plt.subplots(figsize=(10, 6))
            avg_price_bedrooms.plot(kind='line', marker='o', ax=ax, color='red', linewidth=2)
            ax.set_title('Average Price vs Number of Bedrooms')
            ax.set_xlabel('Number of Bedrooms')
            ax.set_ylabel('Average Price (PKR)')
            ax.grid(True, alpha=0.3)
            st.pyplot(fig)

        # ANALYSIS 3: Location-wise Property Count
        st.header("3. 📍 Location-wise Property Distribution")
        
        location_property_count = filtered_df['Location'].value_counts().head(15)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        location_property_count.plot(kind='bar', ax=ax, color='purple', alpha=0.7)
        ax.set_title('Top 15 Locations by Property Count')
        ax.set_xlabel('Location')
        ax.set_ylabel('Number of Properties')
        ax.tick_params(axis='x', rotation=45)
        st.pyplot(fig)

        # ANALYSIS 4: Price vs Bedrooms
        st.header("4. 🔄 Price vs Bedrooms Relationship")
        
        fig, ax = plt.subplots(figsize=(12, 6))
        scatter = ax.scatter(filtered_df['Bedrooms'], filtered_df['Price_numeric'], 
                            alpha=0.6, c=filtered_df['Bedrooms'], cmap='viridis', s=50)
        ax.set_title('Price vs Number of Bedrooms')
        ax.set_xlabel('Number of Bedrooms')
        ax.set_ylabel('Price (PKR)')
        plt.colorbar(scatter, ax=ax, label='Bedrooms')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)

        # ANALYSIS 5: Statistical Summary
        st.header("5. 📋 Statistical Summary & Top Properties")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Numerical Statistics")
            stats_df = filtered_df[['Price_numeric', 'Bedrooms', 'Bathrooms']].describe()
            st.dataframe(stats_df)
        
        with col2:
            st.subheader("Top 10 Most Expensive Properties")
            top_expensive = filtered_df.nlargest(10, 'Price_numeric')[['Location', 'Price', 'Bedrooms', 'Bathrooms']]
            st.dataframe(top_expensive)

        # Raw Data Section
        st.header("📄 Raw Data View")
        
        if st.checkbox("Show filtered data"):
            st.dataframe(filtered_df)
            
            # Download option
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data as CSV",
                data=csv,
                file_name="zameen_karachi_filtered.csv",
                mime="text/csv"
            )

    else:
        st.error("""
        ❌ Failed to load data from database. Please check:
        1. SQL Server is running
        2. Database name is correct
        3. Table exists in the database
        4. Connection parameters are correct
        """)

if __name__ == "__main__":
    # Run the complete pipeline
    main()