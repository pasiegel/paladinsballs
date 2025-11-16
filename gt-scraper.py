import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
import re
import time
import os
import sys  # <-- Added this import


class TeknoParrotScraper:
    def __init__(self, user_ids=None):
        """
        Initialize scraper with list of user IDs
        user_ids: list of user query IDs or path to CSV/JSON file
        """
        self.user_ids = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Load user IDs from various sources
        if user_ids:
            if isinstance(user_ids, list):
                self.user_ids = user_ids
            elif isinstance(user_ids, str):
                if os.path.isfile(user_ids):
                    self.user_ids = self.load_users_from_file(user_ids)
                else:
                    self.user_ids = [user_ids]

    def load_users_from_file(self, filepath):
        """Load user IDs from CSV or JSON file"""
        users = []

        if filepath.endswith('.json'):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                    if isinstance(data, list) and all(isinstance(item, str) for item in data):
                        # Case 1: Simple list of strings
                        # ["user1", "user2"]
                        users = data
                    elif isinstance(data, dict):
                        if 'users' in data and isinstance(data['users'], list):
                            # Case 2: Dict with "users" key (list of strings)
                            # {"users": ["user1", "user2"]}
                            if all(isinstance(item, str) for item in data['users']):
                                users = data['users']
                        elif 'players' in data and isinstance(data['players'], list):
                            # Case 3: Dict with "players" key (list of strings)
                            # {"players": ["user1", "user2"]}
                            if all(isinstance(item, str) for item in data['players']):
                                users = data['players']
                            # Case 4: Dict with "players" key (list of objects)
                            # {"players": [{"id": "user1"}, {"id": "user2"}]}
                            elif all(isinstance(item, dict) and 'id' in item for item in data['players']):
                                users = [player['id'] for player in data['players']]
            except json.JSONDecodeError:
                print(f"Error: Could not decode JSON from {filepath}")
            except FileNotFoundError:
                print(f"Error: File not found at {filepath}")

        elif filepath.endswith('.csv'):
            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Look for user_id, username, queryId, or id column
                        user_id = row.get('user_id') or row.get('username') or row.get('queryId') or row.get('id')
                        if user_id:
                            users.append(user_id)
            except FileNotFoundError:
                print(f"Error: File not found at {filepath}")

        if not users:
            print(f"Warning: Could not find any user IDs in {filepath}. Check file format.")
        else:
            print(f"Loaded {len(users)} users from {filepath}")

        return users

    def fetch_page(self, url):
        """Fetch a page with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return None

    def is_golden_tee_game(self, game_name):
        """Check if the game is a Golden Tee variant"""
        if not game_name:
            return False
        target_games = [
            'golden tee unplugged 2016',
            'power putt live 2013',
            'golden tee live 2006'
        ]
        game_lower = game_name.lower()
        return any(target in game_lower for target in target_games)

    def parse_scorecard(self, html, entry_url):
        """Parse the detailed scorecard page"""
        soup = BeautifulSoup(html, 'html.parser')
        scorecard_data = {
            'entry_url': entry_url
        }

        # Extract game name from the h1 tag (it's in the first table, not the scorecard table)
        game_title = soup.find('h1')
        if game_title:
            scorecard_data['game'] = game_title.get_text(strip=True)

        # Also try to extract username if available
        username_link = soup.find('a', href=re.compile(r'/ProfileViewer/Index/'))
        if username_link:
            username_btn = username_link.find('button', class_='btn-info')
            if username_btn:
                scorecard_data['username'] = username_btn.get_text(strip=True)

        # Find the scorecard table (class='scorecard-table')
        table = soup.find('table', class_='scorecard-table')
        if not table:
            return scorecard_data

        # Parse hole-by-hole data
        holes = []
        distances = []
        pars = []
        player_scores = []

        # Get all rows from tbody
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
        else:
            rows = table.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            if not cells:
                continue

            row_text = [cell.get_text(strip=True) for cell in cells]

            if not row_text:
                continue

            first_cell = row_text[0].upper()

            # Extract distances
            if first_cell == 'DISTANCE':
                distances = row_text[1:]

            # Extract par values
            elif first_cell == 'PAR':
                pars = row_text[1:]

            # Extract player scores
            elif first_cell.startswith('PLAYER'):
                player_num = first_cell.split()[1]
                scores = row_text[1:]
                player_scores.append({
                    'player': player_num,
                    'scores': scores
                })

            # Extract course info
            elif first_cell == 'COURSE:':
                # Course name is in the next cell (colspan=23)
                if len(cells) > 1:
                    scorecard_data['course'] = cells[1].get_text(strip=True)

            # Extract date
            elif first_cell == 'DATE:':
                if len(cells) > 1:
                    scorecard_data['date'] = cells[1].get_text(strip=True)

            # Extract capture ID
            elif first_cell == 'CAPTURE ID:':
                if len(cells) > 1:
                    scorecard_data['capture_id'] = cells[1].get_text(strip=True)

        # Get hole numbers from thead
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                header_cells = header_row.find_all('th')
                # Store the hole headers (e.g., ['Hole', '1', '2', ..., '9', 'OUT', 'IN', 'TOT'])
                holes = [cell.get_text(strip=True) for cell in header_cells]

        # Store structured data
        scorecard_data['holes'] = holes
        scorecard_data['distances'] = distances
        scorecard_data['pars'] = pars
        scorecard_data['players'] = player_scores

        # Extract main player's score (Player 1)
        if player_scores and len(player_scores) > 0:
            main_player = player_scores[0]
            if len(main_player['scores']) > 0:
                # The last few values are OUT, IN, TOT, +/-, GSP
                try:
                    scorecard_data['total_score'] = main_player['scores'][-3] if len(
                        main_player['scores']) > 3 else None
                    scorecard_data['score_vs_par'] = main_player['scores'][-2] if len(
                        main_player['scores']) > 2 else None
                    scorecard_data['gsp'] = main_player['scores'][-1] if len(main_player['scores']) > 0 else None
                except (IndexError, ValueError):
                    pass

        # Extract YouTube video if present
        video_card = soup.find('div', class_='card')
        if video_card:
            # Look for the Video card specifically
            header = video_card.find('h3', class_='card-header')
            if header and 'Video' in header.get_text():
                # Find iframe with YouTube embed
                iframe = video_card.find('iframe')
                if iframe and iframe.get('src'):
                    youtube_url = iframe.get('src')
                    # Convert embed URL to regular YouTube URL
                    if 'youtube.com/embed/' in youtube_url:
                        video_id = youtube_url.split('youtube.com/embed/')[1].split('?')[0]
                        scorecard_data['youtube_video'] = f"https://www.youtube.com/watch?v={video_id}"
                        scorecard_data['youtube_embed'] = youtube_url
                    else:
                        scorecard_data['youtube_video'] = youtube_url

        return scorecard_data

    def extract_entry_links(self, html):
        """Extract Golden Tee entry links from the main leaderboard page"""
        soup = BeautifulSoup(html, 'html.parser')
        entry_links = []

        # Find all links that match the EntrySpecific pattern
        links = soup.find_all('a', href=re.compile(r'EntrySpecific', re.I))

        for link in links:
            href = link.get('href')
            if href:
                # Make absolute URL if needed
                if not href.startswith('http'):
                    href = f"https://teknoparrot.com{href}"

                # Extract game info from link text or nearby elements
                game_name = link.get_text(strip=True)
                parent = link.find_parent(['tr', 'div'])
                if parent:
                    game_elem = parent.find(text=re.compile('Golden Tee|Power Putt', re.I))
                    if game_elem:
                        game_name = game_elem.strip()

                entry_links.append({
                    'url': href,
                    'game': game_name
                })

        return entry_links

    def scrape_user_entries(self, user_id):
        """Scrape all Golden Tee entries for a specific user"""
        base_url = f"https://teknoparrot.com/en/Highscore/UserSpecific?queryId={user_id}"

        print(f"\n{'=' * 60}")
        print(f"Scraping entries for user: {user_id}")
        print(f"{'=' * 60}")

        html = self.fetch_page(base_url)

        if not html:
            print(f"  ✗ Failed to fetch page for user {user_id}")
            return []

        entry_links = self.extract_entry_links(html)

        if not entry_links:
            print(f"  No entry links found for user {user_id}")
            return []

        print(f"  Found {len(entry_links)} total entries")

        user_entries = []

        for i, entry_info in enumerate(entry_links, 1):
            scorecard_html = self.fetch_page(entry_info['url'])
            if not scorecard_html:
                print(f"  ✗ Failed to fetch scorecard {i}/{len(entry_links)}")
                continue

            scorecard_data = self.parse_scorecard(scorecard_html, entry_info['url'])

            # Don't override game name if we already got it from the page
            if not scorecard_data.get('game'):
                scorecard_data['game'] = entry_info['game']

            # Only include Golden Tee games we care about
            if not self.is_golden_tee_game(scorecard_data.get('game', '')):
                continue

            scorecard_data['scraped_at'] = datetime.now().isoformat()
            scorecard_data['query_user_id'] = user_id

            user_entries.append(scorecard_data)

            # Build summary message
            summary_parts = [
                f"{i}/{len(entry_links)}",
                f"Game: {scorecard_data.get('game', 'N/A')}",
                f"Course: {scorecard_data.get('course', 'N/A')}",
                f"Score: {scorecard_data.get('total_score', 'N/A')}"
            ]
            if scorecard_data.get('youtube_video'):
                summary_parts.append("📹")

            print(f"  ✓ {' | '.join(summary_parts)}")

            # Be nice to the server
            time.sleep(1)

        print(f"\n  Collected {len(user_entries)} Golden Tee entries for user {user_id}")
        return user_entries

    def scrape_all_users(self):
        """Scrape all entries for all users"""
        all_entries = []

        if not self.user_ids:
            print("No user IDs loaded. Exiting.")
            return []

        print(f"\n{'#' * 60}")
        print(f"Starting scrape for {len(self.user_ids)} users")
        print(f"{'#' * 60}")

        for idx, user_id in enumerate(self.user_ids, 1):
            print(f"\n[User {idx}/{len(self.user_ids)}]")
            user_entries = self.scrape_user_entries(user_id)
            all_entries.extend(user_entries)

            # Longer delay between users
            if idx < len(self.user_ids):
                time.sleep(2)

        return all_entries

    def save_to_csv(self, entries, filename='golden_tee_leaderboard.csv'):
        """Save entries to CSV file"""
        if not entries:
            print("No entries to save to CSV")
            return

        # Flatten the data for CSV
        flattened = []
        for entry in entries:
            flat_entry = {
                'game': entry.get('game', ''),
                'username': entry.get('username', ''),
                'query_user_id': entry.get('query_user_id', ''),
                'course': entry.get('course', ''),
                'date': entry.get('date', ''),
                'capture_id': entry.get('capture_id', ''),
                'total_score': entry.get('total_score', ''),
                'score_vs_par': entry.get('score_vs_par', ''),
                'gsp': entry.get('gsp', ''),
                'youtube_video': entry.get('youtube_video', ''),
                'youtube_embed': entry.get('youtube_embed', ''),
                'entry_url': entry.get('entry_url', ''),
                'scraped_at': entry.get('scraped_at', '')
            }

            # --- START FIX ---
            # Add individual hole scores for Player 1
            if entry.get('players') and len(entry['players']) > 0:
                player1_scores = entry['players'][0].get('scores', [])
                hole_headers = entry.get('holes', [])  # Get the headers from parse_scorecard

                hole_count = 0

                # hole_headers[0] is 'Hole'. player1_scores[0] is score for hole 1.
                # So, player1_scores[i] corresponds to hole_headers[i+1]

                if len(hole_headers) > 1:  # Check if we have headers at all
                    for i, score in enumerate(player1_scores):
                        # Check if we still have a corresponding header
                        if (i + 1) < len(hole_headers):
                            header = hole_headers[i + 1]

                            # Only add if the header is a digit (e.g., '1', '2', ... '18')
                            if header.isdigit():
                                hole_count += 1
                                flat_entry[f'hole_{hole_count}'] = score
                            # else:
                            # We've hit a non-digit header (like 'OUT' or 'IN'), just ignore it
                            # and continue to the next score/header pair.
                            # pass
                        else:
                            # Ran out of headers. This can happen if scores array
                            # has more items than headers (e.g., +/-, GSP)
                            break
            # --- END FIX ---

            flattened.append(flat_entry)

        # Dynamically get all possible field names for the CSV
        # This handles cases where some entries are 9 holes and others are 18
        all_keys = set()
        for item in flattened:
            all_keys.update(item.keys())

        # Sort keys to have a consistent order
        # Start with standard keys, then add hole keys in order
        standard_keys = [
            'game', 'username', 'query_user_id', 'course', 'date', 'capture_id',
            'total_score', 'score_vs_par', 'gsp', 'youtube_video', 'youtube_embed',
            'entry_url', 'scraped_at'
        ]

        hole_keys = sorted(
            [key for key in all_keys if key.startswith('hole_')],
            key=lambda x: int(x.split('_')[1])
        )

        ordered_keys = standard_keys + hole_keys

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(flattened)
        print(f"\n✓ Saved {len(flattened)} entries to {filename}")

    def save_to_json(self, entries, filename='golden_tee_leaderboard.json'):
        """Save entries to JSON file"""
        if not entries:
            print("No entries to save to JSON")
            return

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {len(entries)} entries to {filename}")


# --- THIS IS THE MODIFIED main() FUNCTION ---
def main():
    # This logic finds the 'users.json' file next to the .exe or .py file
    if getattr(sys, 'frozen', False):
        # Running as a compiled exe
        application_path = os.path.dirname(sys.executable)
    else:
        # Running as a .py script
        try:
            application_path = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            # Fallback for interactive environments (like IDLE)
            application_path = os.path.abspath('.')

    user_json_file_path = os.path.join(application_path, "users.json")

    # Check if the file actually exists before proceeding
    if not os.path.exists(user_json_file_path):
        print(f"Error: 'users.json' not found.")
        print(f"Please place 'users.json' in this directory: {application_path}")
        input("Press Enter to exit...")  # Pause so the user can read the error
        return  # Exit the script

    # Load from the dynamically found path
    scraper = TeknoParrotScraper(user_json_file_path)

    # Scrape all entries
    entries = scraper.scrape_all_users()

    if entries:
        # Save to both CSV and JSON
        scraper.save_to_csv(entries)
        scraper.save_to_json(entries)

        # Print summary by game
        print(f"\n{'=' * 60}")
        print("SUMMARY BY GAME")
        print(f"{'=' * 60}")

        games = {}
        for entry in entries:
            game = entry.get('game', 'Unknown')
            games[game] = games.get(game, 0) + 1

        for game, count in games.items():
            print(f"  {game}: {count} entries")

        print(f"\nTotal entries: {len(entries)}")
    else:
        print("\nNo entries found.")
        input("Press Enter to exit...")  # Pause if no entries were found


if __name__ == "__main__":
    main()
