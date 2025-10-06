from nba_api.stats.endpoints import LeagueDashPlayerStats, PlayerCareerStats
from nba_api.stats.static import players
import pandas as pd
import time
import psycopg2
from sqlalchemy import create_engine
import os
from datetime import datetime

# Database configuration - uses environment variables for security
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'nba_data'),
    'user': os.getenv('DB_USER'),  # No default - must be set in environment
    'password': os.getenv('DB_PASSWORD')  # No default - must be set in environment
}

def validate_db_config():
    """Validate that required database configuration is set."""
    if not DB_CONFIG['user']:
        raise ValueError(
            "DB_USER environment variable is required. "
            "Please set it using: export DB_USER='your_username'"
        )
    if not DB_CONFIG['password']:
        raise ValueError(
            "DB_PASSWORD environment variable is required. "
            "Please set it using: export DB_PASSWORD='your_password'"
        )
    return True

def create_database_if_not_exists():
    """Create the database if it doesn't exist."""
    try:
        validate_db_config()
        # Connect to default postgres database to create new database
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database="postgres",  # Connect to default database
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_CONFIG['database'],)
        )
        
        if cursor.fetchone():
            print(f"✅ Database '{DB_CONFIG['database']}' already exists!")
        else:
            # Create the database
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            print(f"✅ Database '{DB_CONFIG['database']}' created successfully!")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False

def get_db_connection():
    """Create and return a database connection."""
    try:
        # First ensure database exists
        create_database_if_not_exists()
        
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_table_if_not_exists(engine, table_name="player_season_stats"):
    """Create the table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS nba.{table_name} (
        id SERIAL PRIMARY KEY,
        player_id INTEGER,
        player_name VARCHAR(255),
        season VARCHAR(10),
        team_id INTEGER,
        team_abbreviation VARCHAR(10),
        age INTEGER,
        gp INTEGER,
        w INTEGER,
        l INTEGER,
        w_pct DECIMAL(5,3),
        min DECIMAL(8,2),
        fgm DECIMAL(8,2),
        fga DECIMAL(8,2),
        fg_pct DECIMAL(5,3),
        fg3m DECIMAL(8,2),
        fg3a DECIMAL(8,2),
        fg3_pct DECIMAL(5,3),
        ftm DECIMAL(8,2),
        fta DECIMAL(8,2),
        ft_pct DECIMAL(5,3),
        oreb DECIMAL(8,2),
        dreb DECIMAL(8,2),
        reb DECIMAL(8,2),
        ast DECIMAL(8,2),
        tov DECIMAL(8,2),
        stl DECIMAL(8,2),
        blk DECIMAL(8,2),
        blka DECIMAL(8,2),
        pf DECIMAL(8,2),
        pfd DECIMAL(8,2),
        pts DECIMAL(8,2),
        plus_minus DECIMAL(8,2),
        nba_fantasy_pts DECIMAL(8,2),
        dd2 INTEGER,
        td3 INTEGER,
        wnba_fantasy_pts DECIMAL(8,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.commit()
        print(f"Table {table_name} ready!")
    except Exception as e:
        print(f"Error creating table: {e}")

def get_player_career_stats(player_id, player_name=""):
    """Get complete career stats for a specific player."""
    try:
        career = PlayerCareerStats(player_id=player_id)
        df = career.get_data_frames()[0]
        df["player_id"] = player_id
        if player_name:
            df["player_name"] = player_name
        return df
    except Exception as e:
        print(f"Error fetching career for player {player_id}: {e}")
        return pd.DataFrame()

def get_all_players_career_data(save_to_db=True, table_name="player_season_stats"):
    """
    Fetch complete career data for all NBA players.
    This gets ALL seasons for EACH player (not all players for each season).
    """
    # Initialize database connection if saving to DB
    engine = None
    if save_to_db:
        engine = get_db_connection()
        if engine is None:
            print("Could not connect to database. Proceeding without saving to DB.")
            save_to_db = False
        else:
            create_table_if_not_exists(engine, table_name)
    
    # Get list of all NBA players
    print("Fetching list of all NBA players...")
    all_players = players.get_players()
    total_players = len(all_players)
    print(f"Found {total_players} players to process")
    
    all_data = []
    successful_fetches = 0
    failed_fetches = 0
    
    for i, player in enumerate(all_players, 1):
        player_id = player["id"]
        player_name = player["full_name"]
        
        try:
            print(f"Fetching career data for {player_name} ({i}/{total_players})...")
            df = get_player_career_stats(player_id, player_name)
            
            if not df.empty:
                # Save to database if enabled
                if save_to_db and engine is not None:
                    try:
                        # Convert column names to lowercase for database
                        df_db = df.copy()
                        df_db.columns = df_db.columns.str.lower()
                        
                        # Insert data to database (using nba schema)
                        df_db.to_sql(table_name, engine, if_exists='append', index=False, method='multi', schema='nba')
                        print(f"  ✓ Saved {len(df)} career seasons to database")
                    except Exception as db_error:
                        print(f"  ✗ Database error for {player_name}: {db_error}")
                
                all_data.append(df)
                successful_fetches += 1
            else:
                failed_fetches += 1
            
            # Rate limiting - NBA API has limits
            time.sleep(0.6)  # 600ms delay between requests
            
        except Exception as e:
            print(f"Error processing {player_name}: {e}")
            failed_fetches += 1
            time.sleep(3)  # Longer delay on errors
            continue
    
    # Combine all data
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        print(f"\n📊 Career Data Summary:")
        print(f"   Total players processed: {total_players}")
        print(f"   Successful fetches: {successful_fetches}")
        print(f"   Failed fetches: {failed_fetches}")
        print(f"   Total career seasons: {len(full_df)}")
        
        if save_to_db:
            print(f"   Data saved to PostgreSQL table: nba.{table_name}")
        
        return full_df
    else:
        print("No career data fetched!")
        return pd.DataFrame()

def get_all_players_all_seasons(per_mode="PerGame", save_to_db=True, table_name="player_season_stats"):
    """
    Fetch all players' stats for all seasons.
    
    Args:
        per_mode (str): Stats mode - "PerGame", "Totals", "Per36", etc.
        save_to_db (bool): Whether to save data to PostgreSQL
        table_name (str): Database table name
    
    Returns:
        pd.DataFrame: Combined data from all seasons
    """
    # Initialize database connection if saving to DB
    engine = None
    if save_to_db:
        engine = get_db_connection()
        if engine is None:
            print("Could not connect to database. Proceeding without saving to DB.")
            save_to_db = False
        else:
            create_table_if_not_exists(engine, table_name)
    
    # NBA seasons go from 1946-47 to 2024-25
    seasons = []
    for year in range(1946, 2025):
        seasons.append(f"{year}-{str(year+1)[-2:]}")

    all_data = []
    total_seasons = len(seasons)

    for i, season in enumerate(seasons, 1):
        try:
            print(f"Fetching {season} ({i}/{total_seasons})...")
            stats = LeagueDashPlayerStats(season=season, per_mode_detailed=per_mode)
            df = stats.get_data_frames()[0]
            df["SEASON"] = season
            
            # Save to database if enabled
            if save_to_db and engine is not None:
                try:
                    # Convert column names to lowercase for database
                    df_db = df.copy()
                    df_db.columns = df_db.columns.str.lower()
                    
                    # Insert data to database (using nba schema)
                    df_db.to_sql(table_name, engine, if_exists='append', index=False, method='multi', schema='nba')
                    print(f"  ✓ Saved {len(df)} records to database")
                except Exception as db_error:
                    print(f"  ✗ Database error for {season}: {db_error}")
            
            all_data.append(df)
            
            # small delay to respect rate limits
            time.sleep(1)
            
        except Exception as e:
            print(f"Error fetching {season}: {e}")
            time.sleep(3)
            continue

    # Combine all data
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal records fetched: {len(full_df)}")
        
        if save_to_db:
            print(f"Data saved to PostgreSQL table: nba.{table_name}")
        
        return full_df
    else:
        print("No data fetched!")
        return pd.DataFrame()

def main():
    """Main function to run the NBA data ingestion."""
    print("Starting NBA career data ingestion to PostgreSQL...")
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    # Choose which data to fetch:
    df = get_all_players_career_data(
        save_to_db=True,
        table_name="player_season_stats"
    )
    
    print("NBA data ingestion completed!")
    return df

if __name__ == "__main__":
    main()