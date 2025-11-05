import sys
import json
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def parse_date(date_string):
    """Parse date string like 'Sat 18 October 2025' to YYYY-MM-DD format"""
    try:
        # Extract just the date part (remove day of week)
        date_part = ' '.join(date_string.split()[1:])
        date_obj = datetime.strptime(date_part, '%d %B %Y')
        return date_obj.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error parsing date: {e}")
        return None

def clean_team_name(team_name):
    """Remove U18S/U21S suffixes and format team name"""
    # Remove U18S, U21S, etc. (case insensitive)
    cleaned = re.sub(r'\s*U\d+S?$', '', team_name, flags=re.IGNORECASE)
    # Convert to title case (first letter of each word capitalized)
    return cleaned.strip().title()

def parse_player_name(full_name):
    """Split player name into first_name and last_name"""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return parts[0].lower(), ""
    else:
        first_name = parts[0].lower()
        last_name = ' '.join(parts[1:]).lower()
        return first_name, last_name

def get_last_name_for_matching(full_name):
    """Extract the last name from a full name for goal scorer matching"""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ""
    # Always return the last part as the matching key
    return parts[-1].lower()

def fetch_with_retry(url, max_retries=3):
    """Fetch URL with exponential backoff retry logic"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    backoff = 1  # Start with 1 second
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            print(f"Attempt {attempt + 1}/{max_retries + 1}...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Verify we got HTML content
            if response.text and len(response.text) > 100:
                return response
            else:
                last_error = Exception("Received empty or invalid response")
                
        except requests.RequestException as e:
            last_error = e
            print(f"Request failed: {e}")
        
        # If not the last attempt, wait before retrying
        if attempt < max_retries:
            print(f"Retrying in {backoff} seconds...")
            time.sleep(backoff)
            backoff *= 2  # Exponential backoff
        
    # All retries exhausted
    if last_error:
        raise Exception(f"Failed to fetch page after {max_retries + 1} attempts: {last_error}")
    else:
        raise Exception(f"Failed to fetch page after {max_retries + 1} attempts (unknown error)")

def scrape_match(url):
    """Scrape match data from YouthHawk page"""
    
    # Fetch the page with retry logic
    response = fetch_with_retry(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract date from the first table
    # Try multiple methods to find the date
    date_text = None
    
    # Method 1: Look for date in a table cell
    date_cell = soup.find('td', string=re.compile(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\d+\s+\w+\s+\d{4}'))
    if date_cell:
        date_text = date_cell.get_text(strip=True)
    
    # Method 2: Search entire page text for date pattern
    if not date_text:
        page_text = soup.get_text()
        date_match = re.search(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\d+\s+\w+\s+\d{4})', page_text)
        if date_match:
            date_text = date_match.group(0)
    
    if not date_text:
        print("Could not find date in page")
        print("Page content sample:", soup.get_text()[:500])
        return None
    
    match_date = parse_date(date_text)
    print(f"Match date: {match_date}")
    
    # Extract team names and score from the header table
    team_headers = soup.find_all('th', valign='top')
    if len(team_headers) < 3:
        print("Could not find team headers")
        return None
    
    home_team_link = team_headers[0].find('a')
    away_team_link = team_headers[2].find('a')
    score_cell = team_headers[1]
    
    if not home_team_link or not away_team_link:
        print("Could not find team links")
        return None
    
    home_team = clean_team_name(home_team_link.get_text(strip=True))
    away_team = clean_team_name(away_team_link.get_text(strip=True))
    
    # Parse score
    score_text = score_cell.get_text(strip=True)
    score_match = re.search(r'(\d+).*?(\d+)', score_text)
    if not score_match:
        print("Could not parse score")
        return None
    
    home_score = int(score_match.group(1))
    away_score = int(score_match.group(2))
    
    print(f"Teams: {home_team} {home_score}-{away_score} {away_team}")
    
    # Determine win/draw/clean_sheet values for each team
    home_win = 2 if home_score > away_score else 0
    away_win = 2 if away_score > home_score else 0
    home_draw = 1 if home_score == away_score else 0
    away_draw = 1 if home_score == away_score else 0
    home_clean_sheet = 1 if away_score == 0 else 0
    away_clean_sheet = 1 if home_score == 0 else 0
    
    # Extract goal scorers from the score section
    goal_scorers = {}  # {last_name: goal_count}
    
    # Find the row with goal scorers (style font-size:85%)
    goal_row = soup.find('tr', style='font-size:85%')
    
    if goal_row:
        print("DEBUG: Found goal row")
        goal_cells = goal_row.find_all('td')
        print(f"DEBUG: Found {len(goal_cells)} cells in goal row")
        
        # Process cells that likely contain goals (skip the middle score cell)
        for cell_idx, cell in enumerate(goal_cells):
            print(f"\nDEBUG: Processing cell {cell_idx}")
            print(f"DEBUG: Cell HTML: {cell}")
            
            # Find all player links in this cell
            player_links = cell.find_all('a')
            print(f"DEBUG: Found {len(player_links)} player links")
            
            for link in player_links:
                player_name = link.get_text(strip=True)
                # Use the last word of the name for matching (this handles "Giscombe" vs "Naeem Giscombe")
                match_name = get_last_name_for_matching(player_name)
                print(f"DEBUG: Player: {player_name} -> match name: {match_name}")
                
                if not match_name:
                    continue
                
                # Get text immediately after this link until next link or line break
                goal_text = ""
                current = link.next_sibling
                
                while current:
                    # Stop if we hit another link (next player)
                    if hasattr(current, 'name') and current.name == 'a':
                        break
                    # Stop at line breaks
                    if hasattr(current, 'name') and current.name == 'br':
                        break
                    # Accumulate text
                    if isinstance(current, str):
                        goal_text += current
                    current = current.next_sibling
                
                print(f"DEBUG: Goal text for {player_name}: {goal_text}")
                
                # Remove extra info in parentheses like (pen), (o.g), (og), etc.
                cleaned_text = re.sub(r'\([^)]*\)', '', goal_text)
                print(f"DEBUG: Cleaned text: {cleaned_text}")
                
                # Count goal markers (apostrophe followed by digits)
                goal_minutes = re.findall(r"'(\d+)", cleaned_text)
                print(f"DEBUG: Goal minutes found: {goal_minutes}")
                
                if match_name and goal_minutes:
                    # Add to existing count if player already has goals
                    if match_name in goal_scorers:
                        goal_scorers[match_name] += len(goal_minutes)
                    else:
                        goal_scorers[match_name] = len(goal_minutes)
                    print(f"DEBUG: Added {len(goal_minutes)} goals for {match_name}")
                elif match_name and goal_minutes == None:
                    goal_scorers[match_name] += 1
    else:
        print("DEBUG: Could not find goal row")
    
    print(f"\nGoal scorers: {goal_scorers}")
    
    # Parse lineups from both team tables
    team_tables = soup.find_all('table', style='font-size: 90%')
    
    if len(team_tables) < 2:
        print("Could not find team lineup tables")
        return None
    
    players_data = {home_team: [], away_team: []}
    
    # Process each team's table
    for idx, table in enumerate(team_tables[:2]):
        current_team = home_team if idx == 0 else away_team
        
        if current_team == home_team:
            win = home_win
            draw = home_draw
            clean_sheet = home_clean_sheet
        else:
            win = away_win
            draw = away_draw
            clean_sheet = away_clean_sheet
        
        # Find all player rows
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all('td')
            print(f"cells -> {cells}")
            if len(cells) < 3:
                continue
            
            # Check if this is a player row (has position or number)
            player_link = cells[2].find('a')
            if not player_link:
                # this is where the code needs to go to handle single name players
                # player_link = cells[2].text
                # print(f"player_link: {player_link}")
                continue
            
            player_name = player_link.get_text(strip=True)
            print(f"player name: ${player_name}")
            first_name, last_name = parse_player_name(player_name)
            
            if not player_name:
                continue
            
            # Get the last word for goal matching
            match_name = get_last_name_for_matching(player_name)
            
            # Check for substitution markers in the same row
            minutes_played = 90
            is_starter = 1
            is_sub = 0
            
            # Look for Suboff image (substituted off)
            suboff_img = row.find('img', src=re.compile(r'Suboff'))
            if suboff_img:
                # Find the minute in the same cell
                parent_cell = suboff_img.find_parent('td')
                if parent_cell:
                    minute_text = parent_cell.get_text()
                    minute_match = re.search(r"'(\d+)", minute_text)
                    if minute_match:
                        minutes_played = int(minute_match.group(1))
            # Look for Subon image (substituted on)
            subon_img = row.find('img', src=re.compile(r'Subon'))
            if subon_img:
                parent_cell = subon_img.find_parent('td')
                if parent_cell:
                    minute_text = parent_cell.get_text()
                    minute_match = re.search(r"'(\d+)", minute_text)
                    if minute_match:
                        # Only process if we have a valid minute
                        is_starter = 0
                        sub_minute = int(minute_match.group(1))
                        minutes_played = 90 - sub_minute
                        is_sub = 1 if minutes_played >= 15 else 0
                    else:
                        # Subon image but no minute - skip this player
                        continue
            else:
                # Check if this is in the substitutes section but no subon image
                # Look for "SB" or "Substitutes" marker in the row
                is_in_subs_section = False
                
                # Check current row for SB marker
                first_cell = row.find('td')
                if first_cell and 'SB' in first_cell.get_text(strip=True):
                    is_in_subs_section = True
                
                # If we're in the subs section but no subon image, skip
                if is_in_subs_section:
                    continue
            # # Look for Subon image (substituted on)
            # subon_img = row.find('img', src=re.compile(r'Subon'))
            # if subon_img:
            #     is_starter = 0
            #     parent_cell = subon_img.find_parent('td')
            #     if parent_cell:
            #         minute_text = parent_cell.get_text()
            #         minute_match = re.search(r"'(\d+)", minute_text)
            #         if minute_match:
            #             sub_minute = int(minute_match.group(1))
            #             minutes_played = 90 - sub_minute
            #             is_sub = 1 if minutes_played >= 15 else 0
            
            # Calculate goals
            goals = 0
            if match_name in goal_scorers:
                goals = goal_scorers[match_name] * 2
            
            # Apply win/draw/clean_sheet only to starters and subs who played 15+ mins
            player_win = win if (is_starter == 1 or is_sub == 1) else 0
            player_draw = draw if (is_starter == 1 or is_sub == 1) else 0
            player_clean_sheet = clean_sheet if (is_starter == 1 or is_sub == 1) else 0
            
            player_data = {
                "first_name": first_name,
                "last_name": last_name,
                "start": is_starter,
                "minutes_played": minutes_played,
                "sub": is_sub,
                "win": player_win,
                "draw": player_draw,
                "clean_sheet": player_clean_sheet,
                "goals": goals,
                "assists": 0,
                "team_name": current_team,
                "date": match_date
            }
            
            # Check if player already exists (avoid duplicates)
            player_exists = False
            for existing_player in players_data[current_team]:
                if (existing_player['first_name'] == first_name and 
                    existing_player['last_name'] == last_name):
                    player_exists = True
                    break
            
            if not player_exists:
                players_data[current_team].append(player_data)
    
    # Write JSON files
    for team_name, players in players_data.items():
        if players:
            filename = f"{match_date}_{team_name.replace(' ', '_')}.json"
            with open(filename, 'w') as f:
                json.dump(players, f, indent=2)
            print(f"Created {filename} with {len(players)} players")
    
    return players_data

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scraper.py <url>")
        sys.exit(1)
    
    url = sys.argv[1]
    print(f"Scraping: {url}")
    
    try:
        result = scrape_match(url)
        if result:
            print("\nScraping completed successfully!")
        else:
            print("\nScraping failed!")
            sys.exit(1)
    except Exception as e:
        print(f"Error scraping match: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)