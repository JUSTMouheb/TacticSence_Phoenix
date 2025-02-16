import requests
from bs4 import BeautifulSoup
import csv

# URL of the African leagues page on Transfermarkt
url = "https://www.transfermarkt.com/wettbewerbe/afrika"

# Send a GET request to the page
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    print("Successfully fetched the page!")
else:
    print(f"Failed to fetch the page. Status code: {response.status_code}")
    exit()

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.text, "html.parser")

# Find the table containing league data
table = soup.find("table", {"class": "items"})

# Check if the table was found
if not table:
    print("Table not found on the page. Check the HTML structure.")
    exit()

# Initialize a list to store league URLs
league_urls = []

# Loop through each row in the table to extract league URLs
for row in table.find_all("tr")[1:]:  # Skip the header row
    cols = row.find_all("td")

    # Check if the row has enough columns
    if len(cols) >= 3:  # Ensure there are at least 3 columns
        try:
            league_name = cols[2].text.strip()
            league_url = "https://www.transfermarkt.com" + cols[2].find("a")["href"]
            league_urls.append((league_name, league_url))
        except (AttributeError, IndexError) as e:
            print(f"Skipping a row due to error: {e}")
    else:
        print("Skipping a row with insufficient columns.")

# Print the extracted league URLs for debugging
print("Extracted league URLs:")
for league_name, league_url in league_urls:
    print(f"{league_name}: {league_url}")

# Initialize a list to store player data
players = []

# Loop through each league URL to scrape player data
for league_name, league_url in league_urls:
    print(f"\nScraping data for {league_name}...")

    # Send a GET request to the league page
    league_response = requests.get(league_url, headers=headers)
    league_soup = BeautifulSoup(league_response.text, "html.parser")

    # Find the table containing player data
    player_table = league_soup.find("table", {"class": "items"})

    # Check if the player table was found
    if not player_table:
        print(f"Player table not found for {league_name}. Skipping this league.")
        continue

    # Loop through each row in the table to extract player data
    for row in player_table.find_all("tr")[1:]:  # Skip the header row
        cols = row.find_all("td")

        # Check if the row has enough columns
        if len(cols) >= 7:  # Adjust based on the actual number of columns
            try:
                # Extract player details
                name = cols[1].text.strip()
                position = cols[2].text.strip()
                age = cols[3].text.strip()
                nationality = cols[4].img["title"] if cols[4].img else "N/A"
                club = cols[5].img["title"] if cols[5].img else "N/A"
                value = cols[6].text.strip()

                # Append the player data to the list
                players.append({
                    "name": name,
                    "position": position,
                    "age": age,
                    "nationality": nationality,
                    "club": club,
                    "value": value,
                    "league": league_name
                })
            except (AttributeError, IndexError) as e:
                print(f"Skipping a player row due to error: {e}")
        else:
            print("Skipping a player row with insufficient columns.")

# Print the scraped data
print("\nScraped player data:")
for player in players:
    print(
        f"Player: {player['name']}, Position: {player['position']}, Age: {player['age']}, Nationality: {player['nationality']}, Club: {player['club']}, Value: {player['value']}, League: {player['league']}")

# Save the scraped data to a CSV file
try:
    with open("african_players.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["name", "position", "age", "nationality", "club", "value", "league"])
        writer.writeheader()
        writer.writerows(players)
    print("\nData saved to african_players.csv!")
except Exception as e:
    print(f"An error occurred while saving the data: {e}")
