import subprocess
import time
import sys
import os

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 run_scraper_batch.py <path_to_urls_file> [delay_in_seconds]")
        sys.exit(1)

    urls_file = sys.argv[1]
    delay = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0  # default 2 seconds

    if not os.path.isfile(urls_file):
        print(f"Error: file not found -> {urls_file}")
        sys.exit(1)

    with open(urls_file, "r") as file:
        urls = [line.strip() for line in file if line.strip()]

    print(f"Found {len(urls)} URLs in {urls_file}")

    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] Running scraper for: {url}")
        try:
            subprocess.run(["python3", "combined_scraper.py", url], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error while processing {url}: {e}")
        if i < len(urls):
            print(f"Waiting {delay} seconds before next run...")
            time.sleep(delay)

if __name__ == "__main__":
    main()
