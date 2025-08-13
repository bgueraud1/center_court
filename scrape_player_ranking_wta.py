import requests
import pandas as pd
import datetime
import os
import time



# Function to save a DataFrame for a specific date
def save_csv(data, date, save_dir):
    date_str = date.strftime("%Y_%m_%d")
    file_path = os.path.join(save_dir, f"data_{date_str}.csv")
    pd.DataFrame(data).to_csv(file_path, index=False)

# Function to log failed URLs
def log_failed_urls(failed_urls, save_dir):
    failed_path = os.path.join(save_dir, "failed_urls.csv")
    pd.DataFrame({"failed_urls": failed_urls}).to_csv(failed_path, index=False)

# Function to handle API requests with retries
def fetch_data_with_retries(url, retries=5, delay=0):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 429:  # Rate limit
                print(f"Rate limit reached. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            if attempt == retries - 1:
                return None
            time.sleep(delay)
    return None

# Main scraping function
def scrape_data(specific_dates, save_dir):
    failed_urls = []

    for current_date in specific_dates:
        attempt = 0
        max_retries = 7
        date_data = []

        while attempt < max_retries:
            page = 0
            temp_data = []

            print(f"Scraping data for {current_date} (Attempt {attempt + 1}/{max_retries})")
            while page < 72:  # Ensure scraping up to page 72
                url = f"https://api.wtatennis.com/tennis/players/ranked?page={page}&pageSize=20&type=rankSingles&sort=asc&name=&metric=SINGLES&at={current_date}&nationality="
                print(f"Fetching data for date {current_date} - page {page}")
                data = fetch_data_with_retries(url)

                if data is None:
                    print(f"Failed to fetch data for {url}. Logging URL...")
                    failed_urls.append(url)
                    break

                if not data:  # If no data is returned, exit the loop
                    break

                # Extract relevant information from JSON
                for item in data:
                    player = item.get("player")
                    if not player:  # Skip if player data is missing
                        continue
                    temp_data.append({
                        "full_name": player.get("fullName"),
                        "player_id": player.get("id"),
                        "ranking": item.get("ranking"),
                        "points": item.get("points"),
                        "movement": item.get("movement"),
                        "date": current_date.strftime("%Y-%m-%d"),
                    })

                page += 1

            # If sufficient data is collected, break the retry loop
            if len(temp_data) >= 350:
                date_data = temp_data
                break

            attempt += 1
            print(f"Retrying for {current_date} (Attempt {attempt + 1}/{max_retries})")

        # Save the data for the specific date if valid
        if len(date_data) >= 350:
            save_csv(date_data, current_date, save_dir)
        else:
            print(f"Insufficient data for {current_date} after {max_retries} attempts. Logging as failed.")
            failed_urls.append(f"Insufficient data for {current_date}")

        # Save failed URLs periodically
        log_failed_urls(failed_urls, save_dir)

