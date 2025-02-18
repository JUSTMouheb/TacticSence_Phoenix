import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# API-Football credentials
API_KEY = "9fdd4bb6a8482b6f0b7408291e811537"  # Replace with your API key
API_URL = "https://v3.football.api-sports.io/players?league={league_id}&season=2023"

# African League IDs (Replace with correct IDs from API-Football)
AFRICAN_LEAGUE_IDS = [384, 308, 233]  # Example: South Africa, Egypt, Morocco


# Function to fetch player data from API-Football
def fetch_player_data_from_api(retries=3):
    logger.info("Fetching player data from API-Football...")
    headers = {
        "x-apisports-key": API_KEY,
        "x-apisports-host": "v3.football.api-sports.io"
    }
    players = []

    for league_id in AFRICAN_LEAGUE_IDS:
        for attempt in range(retries):
            try:
                response = requests.get(API_URL.format(league_id=league_id), headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    for player in data.get("response", []):
                        stats = player.get("statistics", [{}])[0]
                        players.append({
                            "name": player["player"]["name"],
                            "age": player["player"]["age"],
                            "nationality": player["player"]["nationality"],
                            "position": stats.get("games", {}).get("position", "N/A"),
                            "club": stats.get("team", {}).get("name", "N/A"),
                            "appearances": stats.get("games", {}).get("appearences", 0),
                            "goals": stats.get("goals", {}).get("total", 0),
                            "assists": stats.get("goals", {}).get("assists", 0),
                            "yellow_cards": stats.get("cards", {}).get("yellow", 0),
                            "red_cards": stats.get("cards", {}).get("red", 0),
                            "source": "API-Football"
                        })
                    logger.info(f"Fetched {len(data['response'])} players from league ID {league_id}.")
                    break
                else:
                    logger.warning(
                        f"Attempt {attempt + 1}: Failed to fetch data for league ID {league_id}. Status: {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"Attempt {attempt + 1}: API request error - {e}")
            time.sleep(2)  # Retry delay

    logger.info(f"Total players fetched from API-Football: {len(players)}")
    return players


# Function to scrape player data from Transfermarkt
def scrape_transfermarkt():
    url = "https://www.transfermarkt.com/wettbewerbe/afrika"
    logger.info(f"Scraping Transfermarkt: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()  # Raise an error for HTTP failures
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Transfermarkt: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    league_links = soup.select("table.items tbody tr a[href]")

    league_urls = [f"https://www.transfermarkt.com{a['href']}" for a in league_links][:3]  # Limit to avoid overload

    players = []
    for league_url in league_urls:
        time.sleep(2)  # Avoid being blocked
        logger.info(f"Scraping league: {league_url}")

        try:
            league_response = requests.get(league_url, headers=HEADERS, timeout=10)
            league_response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch league page {league_url}: {e}")
            continue

        league_soup = BeautifulSoup(league_response.text, "html.parser")
        player_rows = league_soup.select("table.items tbody tr")

        if not player_rows:
            logger.warning(f"No player data found for {league_url}")
            continue

        for row in player_rows:
            cols = row.find_all("td")
            if len(cols) < 7:
                continue

            try:
                players.append({
                    "name": cols[1].get_text(strip=True),
                    "position": cols[2].get_text(strip=True),
                    "age": cols[3].get_text(strip=True),
                    "nationality": cols[4].img["title"] if cols[4].img else "N/A",
                    "club": cols[5].img["title"] if cols[5].img else "N/A",
                    "value": cols[6].get_text(strip=True),
                    "source": "Transfermarkt"
                })
            except Exception as e:
                logger.warning(f"Skipped a row due to error: {e}")

    logger.info(f"Scraped {len(players)} players from Transfermarkt.")
    return players


# Save data to CSV

def save_to_csv(data, filename, fieldnames):
    if not data:
        logger.error(f"No data to save for {filename}.")
        return

    try:
        # Save inside the project directory
        project_path = os.path.join(os.getcwd(), filename)

        # Save to desktop (Windows)
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", filename)

        # Save in both locations
        for file_path in [project_path, desktop_path]:
            with open(file_path, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            logger.info(f"Data successfully saved to {file_path}!")

    except Exception as e:
        logger.error(f"Error saving {filename}: {e}")

# Main function to scrape and save data
def main():
    logger.info("Starting scraping process...")

    # Scrape Transfermarkt
    transfermarkt_data = scrape_transfermarkt()

    # Fetch API-Football data
    api_data = fetch_player_data_from_api()

    # Combine data from both sources
    combined_data = transfermarkt_data + api_data

    # Define CSV fields dynamically based on sources
    fieldnames = {
        "name", "position", "age", "nationality", "club", "value",
        "appearances", "goals", "assists", "yellow_cards", "red_cards", "source"
    }

    # Save combined data to CSV
    save_to_csv(combined_data, "african_players.csv", list(fieldnames))

    logger.info("Scraping process completed.")


if __name__ == "__main__":
    main()
