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

async def scrape_multiple_users(usernames, max_tweets=5, stop_date=None):
    """Scrape multiple Twitter users"""
    results = {}
    total_users = len(usernames)
    
    for i, username in enumerate(usernames, 1):
        print(f"\n{'='*60}")
        print(f"Processing user {i}/{total_users}: @{username}")
        print(f"{'='*60}")
        
        success = await test_scraper(username, max_tweets, stop_date)
        results[username] = success
        
        if i < total_users:
            print(f"\nWaiting before next user...")
            await asyncio.sleep(2)  # Add delay between users
    
    # Summary
    print(f"\n{'='*60}")
    print("SCRAPING SUMMARY")
    print(f"{'='*60}")
    successful = sum(1 for success in results.values() if success)
    print(f"Total users processed: {total_users}")
    print(f"Successful: {successful}")
    print(f"Failed: {total_users - successful}")
    
    for username, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} @{username}")
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Twitter scraper for news sources - scrapes predefined list of news accounts')
    parser.add_argument('--max-tweets', type=int, default=5, help='Maximum number of tweets to scrape per user (default: 5)')
    parser.add_argument('--stop-date', type=str, help='Stop when finding tweets from this date (format: YYYY-MM-DD, e.g., 2024-01-15)')
    
    args = parser.parse_args()
    
    # Default list of news sources
    usernames = ['BBCWorld', 'ansa_english', 'nytimes', 'AJEnglish', 'AP', 'WSJ']
    print("Scraping news sources:")
    for username in usernames:
        print(f"  @{username}")
    
    stop_date = None
    if args.stop_date:
        try:
            stop_date = datetime.strptime(args.stop_date, '%Y-%m-%d').date()
            print(f"Stop date set to: {stop_date}")
        except ValueError:
            print("Error: Invalid date format. Please use YYYY-MM-DD format (e.g., 2024-01-15)")
            sys.exit(1)
    
    # Multiple users scraping
    print(f"\nStarting scraping for {len(usernames)} users...")
    results = asyncio.run(scrape_multiple_users(
        usernames=usernames,
        max_tweets=args.max_tweets,
        stop_date=stop_date
    ))
    
    # Check if any failed
    failed_count = sum(1 for success in results.values() if not success)
    if failed_count > 0:
        print(f"\n{failed_count} user(s) failed to scrape")
        sys.exit(1)
    else:
        print("\nAll users scraped successfully!")
