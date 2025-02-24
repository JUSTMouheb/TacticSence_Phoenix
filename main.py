import asyncio
import aiohttp
import logging
from logging.handlers import RotatingFileHandler
import csv
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
from fake_useragent import UserAgent
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import datetime


@dataclass
class Player:
    full_name: str
    nationality: Optional[str] = None
    position: Optional[str] = None
    club: Optional[str] = None
    league: Optional[str] = None
    goals: int = 0
    assists: int = 0
    yellow_cards: int = 0
    red_cards: int = 0
    minutes_played: int = 0
    games_played: int = 0
    date_of_birth: Optional[str] = None
    market_value: Optional[str] = None
    pass_accuracy: Optional[float] = None
    tackles_won: Optional[int] = None
    interceptions: Optional[int] = None
    dribbles_completed: Optional[int] = None
    shots_on_target: Optional[int] = None
    clean_sheets: Optional[int] = None  # For goalkeepers
    saves: Optional[int] = None  # For goalkeepers
    preferred_foot: Optional[str] = None
    height: Optional[int] = None  # In cm
    weight: Optional[int] = None  # In kg
    contract_expiry: Optional[str] = None
    injury_status: Optional[str] = None
    international_caps: Optional[int] = None
    international_goals: Optional[int] = None
    instagram_followers: Optional[int] = None  # Social media popularity
    fifa_rating: Optional[int] = None  # FIFA rating
    weak_foot: Optional[int] = None  # Weak foot rating (1-5)
    skill_moves: Optional[int] = None  # Skill moves rating (1-5)
    transfer_history: Optional[List[str]] = None  # List of previous clubs
    awards: Optional[List[str]] = None  # List of awards


class FootballScraper:
    def __init__(self):
        self.user_agent = UserAgent()
        self.setup_logging()
        self.api_key_football = "9fdd4bb6a8482b6f0b7408291e811537"  # Football API key
        self.api_key_sofascore = "3038f8b59b71db2e5e300d49097d8a124f78b3d5baa261c25dde3e06adcd0872"  # Replace with your SofaScore API key
        self.base_url_football = "https://v3.football.api-sports.io"
        self.base_url_sofascore = "https://api.sofascore.com/api/v1"
        self.session = None
        self.african_leagues = self.load_league_config()

    def setup_logging(self):
        log_file = 'football_scraper.log'
        handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=5)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[handler, logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)

    async def create_session(self):
        self.session = aiohttp.ClientSession(headers={
            'User-Agent': self.user_agent.random
        })

    async def close_session(self):
        if self.session:
            await self.session.close()

    def load_league_config(self) -> Dict:
        return {
            'south_africa': {'id': 384, 'name': 'Premier Soccer League'},
            'egypt': {'id': 308, 'name': 'Egyptian Premier League'},
            'morocco': {'id': 233, 'name': 'Botola Pro'},
            'tunisia': {'id': 173, 'name': 'Tunisian Ligue Professionnelle 1'},
            'algeria': {'id': 174, 'name': 'Algerian Ligue Professionnelle 1'},
            'nigeria': {'id': 306, 'name': 'Nigeria Professional Football League'},
            'ghana': {'id': 376, 'name': 'Ghana Premier League'},
            'senegal': {'id': 377, 'name': 'Senegal Premier League'},
            'ivory_coast': {'id': 378, 'name': 'Ivory Coast Ligue 1'},
            'cameroon': {'id': 379, 'name': 'Cameroon Elite One'},
            # Add more leagues as needed
        }

    def get_current_season(self) -> int:
        # Determine the current season based on the current year
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month

        # Assume the season starts in August and ends in May
        if current_month >= 8:  # August or later
            return current_year
        else:  # January to July
            return current_year - 1

    async def get_latest_season_from_api(self, league_id: int) -> Optional[int]:
        # Fetch available seasons for the league from the API
        url = f"{self.base_url_football}/leagues?id={league_id}"
        headers = {'x-apisports-key': self.api_key_football}
        data = await self.fetch_data(url, headers)

        if not data or not data.get('response'):
            return None

        try:
            # Get the latest season from the API response
            seasons = data['response'][0]['seasons']
            latest_season = max(season['year'] for season in seasons)
            return latest_season
        except Exception as e:
            self.logger.error(f"Error fetching latest season for league {league_id}: {str(e)}")
            return None

    async def fetch_data(self, url: str, headers: Optional[Dict] = None) -> Optional[Dict]:
        try:
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    self.logger.warning("Rate limit hit. Waiting before retry...")
                    await asyncio.sleep(60)
                    return None
                else:
                    self.logger.warning(f"Request failed with status {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return None

    async def get_player_details_football(self, player_id: int, season: int) -> Optional[Player]:
        url = f"{self.base_url_football}/players?id={player_id}&season={season}"
        headers = {'x-apisports-key': self.api_key_football}
        data = await self.fetch_data(url, headers)

        if not data or not data.get('response'):
            return None

        try:
            player_data = data['response'][0]
            statistics = player_data.get('statistics', [{}])[0]

            return Player(
                full_name=player_data['player']['name'],
                nationality=player_data['player']['nationality'],
                date_of_birth=player_data['player'].get('birth', {}).get('date'),
                position=statistics.get('games', {}).get('position'),
                club=statistics.get('team', {}).get('name'),
                league=statistics.get('league', {}).get('name'),
                goals=statistics.get('goals', {}).get('total', 0) or 0,
                assists=statistics.get('goals', {}).get('assists', 0) or 0,
                yellow_cards=statistics.get('cards', {}).get('yellow', 0) or 0,
                red_cards=statistics.get('cards', {}).get('red', 0) or 0,
                minutes_played=statistics.get('games', {}).get('minutes', 0) or 0,
                games_played=statistics.get('games', {}).get('appearences', 0) or 0
            )
        except Exception as e:
            self.logger.error(f"Error processing player {player_id}: {str(e)}")
            return None

    async def scrape_league_football(self, league_id: int) -> List[Player]:
        # Fetch the latest season for the league
        latest_season = await self.get_latest_season_from_api(league_id)
        if not latest_season:
            self.logger.warning(f"No season data found for league {league_id}")
            return []

        url = f"{self.base_url_football}/players?league={league_id}&season={latest_season}"
        headers = {'x-apisports-key': self.api_key_football}
        data = await self.fetch_data(url, headers)

        if not data or not data.get('response'):
            self.logger.warning(f"No data found for league {league_id} in season {latest_season}")
            return []

        players = []
        tasks = []

        for player_data in data['response']:
            player_id = player_data['player']['id']
            tasks.append(self.get_player_details_football(player_id, latest_season))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        players = [r for r in results if isinstance(r, Player)]

        self.logger.info(f"Scraped {len(players)} players from league {league_id} (season {latest_season})")
        return players

    def scrape_transfermarkt(self) -> List[Player]:
        url = "https://www.transfermarkt.com/wettbewerbe/afrika"
        self.logger.info(f"Scraping Transfermarkt: {url}")

        try:
            response = requests.get(url, headers={'User-Agent': self.user_agent.random}, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch Transfermarkt: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        league_links = soup.select("table.items tbody tr a[href]")
        league_urls = [f"https://www.transfermarkt.com{a['href']}" for a in league_links]

        players = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(self.scrape_transfermarkt_league, url): url for url in league_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    players.extend(future.result())
                except Exception as e:
                    self.logger.error(f"Error scraping {url}: {e}")

        self.logger.info(f"Scraped {len(players)} players from Transfermarkt.")
        return players

    def scrape_transfermarkt_league(self, league_url: str) -> List[Player]:
        self.logger.info(f"Scraping league: {league_url}")

        try:
            response = requests.get(league_url, headers={'User-Agent': self.user_agent.random}, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch league page {league_url}: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        player_rows = soup.select("table.items tbody tr")

        players = []
        for row in player_rows:
            cols = row.find_all("td")
            if len(cols) < 7:
                continue

            try:
                players.append(Player(
                    full_name=cols[1].get_text(strip=True),
                    position=cols[2].get_text(strip=True),
                    nationality=cols[4].img["title"] if cols[4].img else "N/A",
                    club=cols[5].img["title"] if cols[5].img else "N/A",
                    market_value=cols[6].get_text(strip=True)
                ))
            except Exception as e:
                self.logger.warning(f"Skipped a row due to error: {e}")

        return players

    def save_players(self, players: List[Player], filename: str):
        if not players:
            self.logger.warning("No players to save.")
            return

        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=Player.__annotations__.keys())
                writer.writeheader()
                for player in players:
                    writer.writerow(asdict(player))
            self.logger.info(f"Successfully saved {len(players)} players to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving players: {str(e)}")

    async def run(self):
        self.logger.info("Starting football player scraping")
        await self.create_session()

        try:
            # Scrape API-Football data for African leagues
            all_players = []
            tasks = [
                self.scrape_league_football(league['id'])
                for league in self.african_leagues.values()
            ]
            results = await asyncio.gather(*tasks)
            for players in results:
                all_players.extend(players)

            # Scrape Transfermarkt data for African leagues
            transfermarkt_players = self.scrape_transfermarkt()
            all_players.extend(transfermarkt_players)

            # Remove duplicates
            unique_players = list({(p.full_name, p.club): p for p in all_players}.values())

            self.logger.info(f"Total players scraped: {len(unique_players)}")
            self.save_players(unique_players, 'african_players.csv')

        finally:
            await self.close_session()


async def main():
    scraper = FootballScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())