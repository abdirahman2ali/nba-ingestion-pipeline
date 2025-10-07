# 🏀 NBA Data Pipeline

**Scrape all NBA player season averages from Basketball Reference** → PostgreSQL database

## ✨ What it does

- 🔍 **Fetches per-game season averages** for all NBA players (1950-present)
- 💾 **Saves directly to PostgreSQL** with proper schema and indexing
- 🛡️ **Smart rate limiting** - respects Basketball Reference
- 🔄 **Configurable** - easily adjust year ranges
- 📊 **Chunked inserts** - efficient batch processing (500 rows/chunk)

## 🎯 Perfect for

- Building NBA analytics dashboards
- Historical player comparisons  
- Career trajectory analysis
- Sports analytics research
- Machine learning models

## 📊 The Data

Every player's per-game season averages: points, rebounds, assists, steals, blocks, FG%, 3PT%, FT%, and more. Season-level aggregates from 1947 to present day.

**Data includes:**
- Player name, position, age, team
- Games played (GP), games started (GS), minutes per game
- Shooting stats (FG, FGA, FG%, 3P, 3PA, 3P%, 2P, 2PA, 2P%, eFG%)
- Free throw stats (FT, FTA, FT%)
- Rebounds (ORB, DRB, TRB)
- Assists, steals, blocks, turnovers, personal fouls
- Points per game

## ⏱️ Runtime

- ~1 second per season (rate limited)
- 2000-2025 (26 seasons): ~30 seconds
- 1950-2025 (76 seasons): ~1.5 minutes

## 📦 Output

**PostgreSQL table**: `nba.player_season_averages`
- 30,462+ records (1950-2025)
- 4,775+ unique players
- Indexed by player and season for fast queries

## 🛠️ Tech Stack

- **Basketball Reference** - Data source (web scraping)
- **Pandas** - Data manipulation
- **PostgreSQL** - Database storage
- **SQLAlchemy** - Database ORM
- **BeautifulSoup** - HTML parsing