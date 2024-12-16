import asyncio
import aiohttp

# Constants for APIs
LASTFM_API_URL = "https://ws.audioscrobbler.com/2.0/"
LISTENBRAINZ_API_URL = "https://api.listenbrainz.org/1/submit-listens"

LASTFM_API_KEY = "your_lastfm_api_key" #please make changes
LISTENBRAINZ_TOKEN = "your_listenbrainz_user_token" #please make changes

LASTFM_BATCH_LIMIT = 200  # Maximum limit for scrobbles per request
LISTENBRAINZ_RATE_LIMIT = 10  # Max requests per second (example)

# Fetch scrobbles from Last.fm asynchronously
async def fetch_scrobbles(session, user, page):
    params = {
        "method": "user.getRecentTracks",
        "user": user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": LASTFM_BATCH_LIMIT,
        "page": page,
    }
    async with session.get(LASTFM_API_URL, params=params) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f"Failed to fetch page {page}: {response.status}")
            return None

# Submit scrobbles to ListenBrainz asynchronously
async def submit_scrobbles(session, scrobbles):
    headers = {"Authorization": f"Token {LISTENBRAINZ_TOKEN}"}
    payload = {"listens": scrobbles}
    async with session.post(LISTENBRAINZ_API_URL, json=payload, headers=headers) as response:
        if response.status == 200:
            print(f"Successfully submitted {len(scrobbles)} scrobbles.")
        else:
            print(f"Failed to submit scrobbles: {response.status}")

# Process scrobbles in parallel
async def process_scrobbles(lastfm_user, total_pages):
    async with aiohttp.ClientSession() as session:
        # Step 1: Fetch scrobbles from Last.fm in parallel
        fetch_tasks = [
            fetch_scrobbles(session, lastfm_user, page)
            for page in range(1, total_pages + 1)
        ]
        results = await asyncio.gather(*fetch_tasks)
        
        # Flatten results and extract scrobbles
        all_scrobbles = []
        for result in results:
            if result and "recenttracks" in result:
                all_scrobbles.extend(result["recenttracks"].get("track", []))

        print(f"Fetched {len(all_scrobbles)} scrobbles.")

        # Step 2: Submit scrobbles to ListenBrainz in rate-compliant batches
        batch_size = LISTENBRAINZ_RATE_LIMIT
        for i in range(0, len(all_scrobbles), batch_size):
            batch = all_scrobbles[i:i + batch_size]
            await submit_scrobbles(session, batch)
            await asyncio.sleep(1)  # Enforce rate limit,listenBrainz team it helps in providing delay 

# Main entry point
if __name__ == "__main__":
    lastfm_user = "your_lastfm_username" #please make changes
    total_pages = 5  # Adjust based on the total number of scrobbles / LASTFM_BATCH_LIMIT 

    asyncio.run(process_scrobbles(lastfm_user, total_pages))
