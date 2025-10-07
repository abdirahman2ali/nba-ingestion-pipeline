import pandas as pd
import time
import psycopg2
from sqlalchemy import create_engine, text
import os
from bs4 import BeautifulSoup
import requests

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'nba_data'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def validate_db_config():
    """Validate that required database credentials are set."""
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
    """Create the PostgreSQL database if it doesn't exist."""
    try:
        validate_db_config()
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database="postgres",
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_CONFIG['database'],)
        )
        
        if cursor.fetchone():
            print(f"✅ Database '{DB_CONFIG['database']}' already exists!")
        else:
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            print(f"✅ Database '{DB_CONFIG['database']}' created successfully!")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False

def create_schema_if_not_exists(engine):
    """Create the nba schema if it doesn't exist."""
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS nba"))
            conn.commit()
        print("✅ Schema 'nba' ready!")
        return True
    except Exception as e:
        print(f"❌ Error creating schema: {e}")
        return False

def get_db_connection():
    """Establish connection to the PostgreSQL database."""
    try:
        create_database_if_not_exists()
        
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        
        # Create schema
        create_schema_if_not_exists(engine)
        
        return engine
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None

def get_season_stats(season_end_year, data_format='PER_GAME'):
    """
    Scrape per-game season stats for all players from Basketball Reference.
    
    Args:
        season_end_year: The year the season ended (e.g., 2024 for 2023-24 season)
        data_format: Type of stats ('PER_GAME', 'TOTALS', 'PER_36MIN', 'PER_100POSS', 'ADVANCED')
    
    Returns:
        DataFrame with all players' stats for that season
    """
    format_map = {
        'PER_GAME': 'per_game',
        'TOTALS': 'totals',
        'PER_36MIN': 'per_minute',
        'PER_100POSS': 'per_poss',
        'ADVANCED': 'advanced'
    }
    
    stat_type = format_map.get(data_format.upper(), 'per_game')
    url = f'https://www.basketball-reference.com/leagues/NBA_{season_end_year}_{stat_type}.html'
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the stats table
        table = soup.find('table', {'id': f'{stat_type}_stats'})
        
        if not table:
            return pd.DataFrame()
        
        # Read table into DataFrame
        df = pd.read_html(str(table))[0]
        
        # Clean up the DataFrame
        # Remove header rows that repeat in the table
        df = df[df['Player'] != 'Player']
        
        # Drop the 'Rk' (Rank) column if it exists
        if 'Rk' in df.columns:
            df = df.drop('Rk', axis=1)
        
        df = df.reset_index(drop=True)
        
        return df
        
    except Exception as e:
        raise Exception(f"Error fetching data for {season_end_year}: {str(e)}")

def create_table_if_not_exists(engine, table_name="player_season_averages"):
    """Create the player season averages table if it doesn't exist."""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS nba.{table_name} (
        id SERIAL PRIMARY KEY,
        player VARCHAR(255),
        age DECIMAL(5,2),
        team VARCHAR(10),
        pos VARCHAR(10),
        g DECIMAL(5,1),
        fg DECIMAL(5,2),
        fga DECIMAL(5,2),
        fg_pct DECIMAL(5,3),
        ft DECIMAL(5,2),
        fta DECIMAL(5,2),
        ft_pct DECIMAL(5,3),
        ast DECIMAL(5,2),
        pf DECIMAL(5,2),
        pts DECIMAL(5,2),
        awards TEXT,
        season VARCHAR(10),
        trb DECIMAL(5,2),
        mp DECIMAL(5,2),
        gs DECIMAL(5,1),
        orb DECIMAL(5,2),
        drb DECIMAL(5,2),
        stl DECIMAL(5,2),
        blk DECIMAL(5,2),
        tov DECIMAL(5,2),
        three_p DECIMAL(5,2),
        three_pa DECIMAL(5,2),
        three_p_pct DECIMAL(5,3),
        two_p DECIMAL(5,2),
        two_pa DECIMAL(5,2),
        two_p_pct DECIMAL(5,3),
        efg_pct DECIMAL(5,3),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_player_season ON nba.{table_name}(player, season);
    CREATE INDEX IF NOT EXISTS idx_season ON nba.{table_name}(season);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print(f"✅ Table nba.{table_name} ready!")
        return True
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False

def get_all_seasons(start_year=1950, end_year=2025, save_to_db=True, table_name="player_season_averages"):
    """
    Fetch all NBA player per-game season averages using Basketball Reference Scraper.
    
    Args:
        start_year: First season end year to fetch (default: 1950)
        end_year: Last season end year to fetch (default: 2025)
        save_to_db: Whether to save data to PostgreSQL (default: True)
        table_name: Name of the database table (default: player_season_averages)
    
    Returns:
        DataFrame with all fetched season averages
    """
    engine = None
    if save_to_db:
        engine = get_db_connection()
        if engine is None:
            print("⚠️  Could not connect to database. Proceeding without saving to DB.")
            save_to_db = False
        else:
            create_table_if_not_exists(engine, table_name)
    
    print(f"\n🏀 NBA Season Averages Ingestion - Basketball Reference Scraper")
    print(f"📅 Season range: {start_year-1}-{start_year} to {end_year-1}-{end_year}")
    print(f"📊 Total seasons: {end_year - start_year + 1}")
    if save_to_db:
        print(f"💾 Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        print(f"📋 Table: nba.{table_name}")
    print(f"⏱️  Estimated time: ~{(end_year - start_year + 1)} seconds (with rate limiting)\n")
    
    all_data = []
    successful_seasons = 0
    failed_seasons = []
    
    for year in range(start_year, end_year + 1):
        try:
            print(f"📊 Fetching {year-1}-{year} season... ", end="", flush=True)
            
            # Fetch per-game season stats from Basketball Reference
            df = get_season_stats(season_end_year=year, data_format='PER_GAME')
            
            if df is None or df.empty:
                print(f"⚠️  No data returned for {year}")
                failed_seasons.append(year)
                time.sleep(1)
                continue
            
            # Add season identifier
            df["SEASON"] = f"{year-1}-{str(year)[-2:]}"
            
            print(f"✅ {len(df):,} players", end="")
            
            # Save to database if enabled
            if save_to_db and engine is not None:
                try:
                    df_db = df.copy()
                    # Clean column names for PostgreSQL compatibility
                    df_db.columns = (df_db.columns
                                     .str.lower()
                                     .str.replace('%', '_pct')
                                     .str.replace('3p', 'three_p')
                                     .str.replace('2p', 'two_p'))
                    
                    # Insert with chunking to avoid too many parameters error
                    df_db.to_sql(
                        table_name, 
                        engine, 
                        if_exists='append', 
                        index=False, 
                        method='multi',
                        chunksize=500,  # Insert in batches of 500 rows
                        schema='nba'
                    )
                    print(f" → 💾 Saved to DB")
                except Exception as db_error:
                    print(f" → ❌ DB Error: {str(db_error)[:100]}")
            else:
                print()
            
            all_data.append(df)
            successful_seasons += 1
            
            # Respect rate limits
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            failed_seasons.append(year)
            time.sleep(3)  # Longer delay on error
            continue
    
    # Compile results
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        
        print(f"\n{'='*60}")
        print(f"🎉 INGESTION COMPLETE!")
        print(f"{'='*60}")
        print(f"✅ Successful seasons: {successful_seasons}/{end_year - start_year + 1}")
        if failed_seasons:
            print(f"❌ Failed seasons: {failed_seasons}")
        print(f"📊 Total records: {len(full_df):,}")
        # Check for column name variations
        player_col = next((col for col in ['Player', 'PLAYER'] if col in full_df.columns), None)
        team_col = next((col for col in ['Tm', 'TM', 'Team'] if col in full_df.columns), None)
        season_col = next((col for col in ['SEASON', 'Season'] if col in full_df.columns), None)
        
        if player_col:
            print(f"👥 Unique players: {full_df[player_col].nunique():,}")
        if team_col:
            print(f"🏟️  Unique teams: {full_df[team_col].nunique()}")
        if season_col:
            print(f"📅 Seasons: {full_df[season_col].min()} to {full_df[season_col].max()}")
        
        if save_to_db:
            print(f"💾 Data saved to: nba.{table_name}")
        
        print(f"{'='*60}\n")
        
        return full_df
    else:
        print("\n❌ No data fetched!")
        return pd.DataFrame()

def main():
    """Main execution function."""
    print("🏀 NBA Season Averages Ingestion Pipeline")
    print("📚 Using Basketball Reference Scraper")
    print(f"{'='*60}\n")
    
    # Configuration
    START_YEAR = 1950  # Change this to fetch different range (1950 is first available year)
    END_YEAR = 2025
    SAVE_TO_DB = True  # Save to PostgreSQL database
    TABLE_NAME = "player_season_averages"
    
    # Fetch all seasons and save directly to database
    df = get_all_seasons(
        start_year=START_YEAR,
        end_year=END_YEAR,
        save_to_db=SAVE_TO_DB,
        table_name=TABLE_NAME
    )
    
    # Display sample data
    if not df.empty:
        print("\n📋 Sample Data (first 5 records):")
        print(df.head())
        print(f"\n📊 DataFrame shape: {df.shape}")
        print(f"\n📝 Columns: {list(df.columns)}")
    
    return df

if __name__ == "__main__":
    main()