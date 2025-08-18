#!/usr/bin/env python3


import asyncio
import sys
import os
import argparse
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from scraper import scrape_twitter

async def test_scraper(username, max_tweets=5, stop_date=None):
    
    test_username = username
    
    try:
        print(f"Starting scrape for @{test_username}")
        if stop_date:
            print(f"Will stop when reaching {max_tweets} tweets or finding tweets from {stop_date}")
        else:
            print(f"Will stop when reaching {max_tweets} tweets")
            
        result = await scrape_twitter(
            username=test_username,
            max_tweets=max_tweets,
            max_retweets=0,  # Skip retweets
            max_followers=0,  # Skip followers
            max_following=0,   # Skip following
            stop_date=stop_date  # Add stop date parameter
        )
        
        print(f"\nTest completed successfully!")
        print(f"Profile: {result.get('user_profile', {})}")
        print(f"Tweets found: {len(result.get('tweets', []))}")
        # Skip displaying retweets, followers, and following counts
        
        return True
        
    except Exception as e:
        print(f"Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Twitter scraper with specified username - only scrapes tweets')
    parser.add_argument('username', help='Twitter username to scrape (without @)')
    parser.add_argument('--max-tweets', type=int, default=5, help='Maximum number of tweets to scrape (default: 5)')
    parser.add_argument('--stop-date', type=str, help='Stop when finding tweets from this date (format: YYYY-MM-DD, e.g., 2024-01-15)')
    
    args = parser.parse_args()
    
    stop_date = None
    if args.stop_date:
        try:
            stop_date = datetime.strptime(args.stop_date, '%Y-%m-%d').date()
            print(f"Stop date set to: {stop_date}")
        except ValueError:
            print("Error: Invalid date format. Please use YYYY-MM-DD format (e.g., 2024-01-15)")
            sys.exit(1)
    
    success = asyncio.run(test_scraper(
        username=args.username,
        max_tweets=args.max_tweets,
        stop_date=stop_date
    ))
    if success:
        print("\nTest passed - scraper appears to be working!")
    else:
        print("\nTest failed - please check the errors above")
        sys.exit(1)
