import os
import json
import asyncio
import random
import hashlib
import time
import glob
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple, Set
from playwright.async_api import async_playwright, TimeoutError, Page
from pathlib import Path


COOKIES_FILE = os.path.join(os.path.dirname(__file__), 'twitter_cookies.json')



SCROLL_DELAY = 2  
REQUEST_DELAY = 0.5  
MAX_RETRIES = 2
TIMEOUT = 10000 

def parse_tweet_date(tweet_date_str: str) -> Optional[date]:
    """Parse tweet date string to date object for comparison."""
    if not tweet_date_str or tweet_date_str == "Unknown":
        return None
    
    try:
        date_formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  
            '%Y-%m-%dT%H:%M:%SZ',     
            '%Y-%m-%d %H:%M:%S',      
            '%Y-%m-%d',               
            '%b %d, %Y',              
            '%B %d, %Y',              
            '%d %b %Y',               
            '%m/%d/%Y',               
        ]
        
        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(tweet_date_str, date_format).date()
                return parsed_date
            except ValueError:
                continue
        
        try:
            current_year = datetime.now().year
            parsed_date = datetime.strptime(f"{tweet_date_str} {current_year}", '%b %d %Y').date()
            return parsed_date
        except ValueError:
            pass
            
        print(f"Could not parse date format: {tweet_date_str}")
        return None
        
    except Exception as e:
        print(f"Error parsing date '{tweet_date_str}': {str(e)}")
        return None

def should_stop_at_date(tweet_date_str: str, stop_date) -> bool:
    """Check if we should stop scraping based on tweet date."""
    if not stop_date or not tweet_date_str:
        return False
    
    tweet_date = parse_tweet_date(tweet_date_str)
    if not tweet_date:
        return False
    
    return tweet_date <= stop_date

async def safe_wait_for_selector(page: Page, selector: str, timeout: int = TIMEOUT, description: str = "element") -> bool:
    """Safely wait for a selector with proper error handling."""
    try:
        timeout_seconds = max(timeout/1000, 10)  
        await asyncio.wait_for(
            page.wait_for_selector(selector, timeout=timeout, state="attached"),
            timeout=timeout_seconds + 5 
        )
        return True
    except asyncio.TimeoutError:
        print(f"Timeout waiting for {description} (selector: {selector}) - {timeout}ms")
        return False
    except TimeoutError:
        print(f"Playwright timeout waiting for {description} (selector: {selector}) - {timeout}ms")
        return False
    except Exception as e:
        print(f"Error waiting for {description}: {str(e)}")
        return False

async def safe_screenshot(element, path: str, description: str = "element") -> str:
    """Safely take a screenshot with error handling."""
    try:
        await asyncio.wait_for(element.screenshot(path=path), timeout=5)
        return path
    except asyncio.TimeoutError:
        print(f"Screenshot timeout for {description}")
        return ""
    except Exception as e:
        print(f"Could not take screenshot for {description}: {str(e)}")
        return ""

async def safe_operation(operation, timeout_seconds=10, description="operation"):
    """Safely execute an operation with timeout."""
    try:
        return await asyncio.wait_for(operation, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        print(f"Operation timeout: {description}")
        return None
    except Exception as e:
        print(f"Operation error: {description} - {str(e)}")
        return None

async def safe_browser_close(browser):
    """Safely close browser with timeout."""
    try:
        await asyncio.wait_for(browser.close(), timeout=5)
        print("Browser closed successfully")
    except asyncio.TimeoutError:
        print("Browser close timeout - force terminating")
    except Exception as e:
        print(f"Error closing browser: {str(e)}")

async def rate_limit_delay(delay: float = REQUEST_DELAY) -> None:
    """Add delay for rate limiting."""
    await asyncio.sleep(delay)

def generate_secure_tweet_id(tweet_element_html: str, fallback_content: str = "") -> str:
    """Generate a secure, unique tweet ID using hashing."""
    content = tweet_element_html + fallback_content + str(time.time())
    return hashlib.md5(content.encode()).hexdigest()[:16]

def clean_username_for_filename(username: str) -> str:
    """Clean username to be filesystem-safe."""
    cleaned = username.replace('@', '').replace(' ', '_').replace('/', '_').replace('\\', '_')
    cleaned = ''.join(c for c in cleaned if c.isalnum() or c in '_-')
    return cleaned

def generate_unique_filename(username: str, content_type: str, index: int) -> str:
    """Generate a unique filename with timestamp (screenshots disabled)."""
    cleaned_username = clean_username_for_filename(username)
    timestamp = int(time.time())
    return f"{cleaned_username}_{content_type}_{index}_{timestamp}.txt" 

async def wait_for_profile_load(page: Page, username: str) -> bool:
    """Wait for profile to load with multiple fallback strategies."""
    try:
        if await safe_wait_for_selector(page, 'article[data-testid="tweet"]', timeout=3000, description="tweets"):
            return True
        
        if await safe_wait_for_selector(page, 'div[data-testid="emptyState"]', timeout=2000, description="empty state"):
            print(f"Profile {username} has no tweets or is empty")
            return True
            
        if await safe_wait_for_selector(page, 'div[data-testid="UserName"]', timeout=3000, description="profile header"):
            print(f"Profile {username} loaded but may be private")
            return True
            
        print(f"Timeout waiting for profile {username} to load")
        return False
    except Exception as e:
        print(f"Error waiting for profile load: {str(e)}")
        return False

async def scrape_user_profile(page: Page, username: str) -> Dict[str, str]:
    """Scrape user profile information with improved error handling."""
    try:
        print(f"Navigating to profile page for @{username}...")
        await page.goto(f"https://twitter.com/{username}", wait_until="domcontentloaded", timeout=TIMEOUT)
        await rate_limit_delay()
        
        if not await safe_wait_for_selector(page, 'div[data-testid="UserName"]', description="profile"):
            print(f"Could not load profile for @{username}")
            return {"username": username, "bio": ""}
        
        display_name = ""
        name_selectors = [
            'div[data-testid="UserName"] span',
            'div[data-testid="UserName"] div span',
            'h1[data-testid="UserName"] span'
        ]
        
        for selector in name_selectors:
            try:
                name_element = page.locator(selector).first
                if await name_element.count() > 0:
                    display_name = await name_element.inner_text()
                    if display_name:
                        break
            except Exception as e:
                print(f"Error with name selector {selector}: {str(e)}")
                continue
        
        bio = ""
        bio_selectors = [
            'div[data-testid="UserDescription"]',
            'div[data-testid="UserBio"]',
            'div[data-testid="UserProfessionalCategory"]'
        ]
        
        for attempt in range(MAX_RETRIES):
            for selector in bio_selectors:
                try:
                    bio_element = page.locator(selector)
                    if await bio_element.count() > 0:
                        bio = await bio_element.inner_text()
                        if bio:
                            break
                except Exception as e:
                    print(f"Error with bio selector {selector}: {str(e)}")
                    continue
            
            if bio:
                break
            await rate_limit_delay()
        
        return {
            "username": display_name or username,
            "bio": bio.strip() if bio else ""
        }

    except Exception as e:
        print(f"Error scraping profile: {str(e)}")
        return {"username": username, "bio": ""}

async def get_tweet_content(tweet_element) -> str:
    """Helper function to get tweet content from any tweet element"""
    try:
        content_element = tweet_element.locator('div[data-testid="tweetText"]').first
        if await content_element.count() > 0:
            return await content_element.inner_text()
    except Exception as e:
        print(f"Could not get tweet text: {str(e)}")
    return ""

async def get_quoted_tweet_info(tweet_element) -> Optional[Dict[str, str]]:
    """Get information about a quoted tweet if present"""
    try:
        quoted_container = tweet_element.locator('div:has(> div[data-testid="tweet"])').last
        if await quoted_container.count() > 0:
            quoted_text = await quoted_container.locator('div[data-testid="tweetText"]').inner_text()
            name_element = quoted_container.locator('div[data-testid="User-Name"] div span').first
            quoted_username = await name_element.inner_text() if await name_element.count() > 0 else ""
            return {
                "quoted_content": quoted_text,
                "quoted_username": quoted_username
            }
    except Exception as e:
        print(f"Could not get quoted tweet info: {str(e)}")
    return None

async def get_main_tweet_content(tweet_element) -> str:
    """Get the main tweet content with improved selector robustness and timeout protection."""
    content_selectors = [
        'div[data-testid="tweetText"]',
        'div[lang]:not([data-testid])',
        'article div[lang]',
        'div[role="article"] div[lang]',
        'div[dir="auto"]:not([data-testid])',
        'span[lang]'
    ]
    
    for selector in content_selectors:
        try:
            elements = tweet_element.locator(selector)
            count = await asyncio.wait_for(elements.count(), timeout=3)
            if count > 0:
                texts = []
                for i in range(min(count, 3)):  
                    try:
                        element = elements.nth(i)
                        text = await asyncio.wait_for(element.inner_text(), timeout=2)
                        if text and len(text.strip()) > 0:
                            texts.append(text.strip())
                    except (asyncio.TimeoutError, Exception):
                        continue
                
                if texts:
                    return " ".join(texts)
        except (asyncio.TimeoutError, Exception):
            continue
    
    try:
        text = await asyncio.wait_for(tweet_element.inner_text(), timeout=3)
        if text and len(text.strip()) > 10:  # Avoid getting just metadata
            return text.strip()[:500]  # Limit length
    except (asyncio.TimeoutError, Exception):
        pass
    
    return ""

async def get_tweet_date(tweet_element) -> str:
    """Extract the date/time when the tweet was posted with timeout protection."""
    try:
        time_element = tweet_element.locator('time').first
        count = await asyncio.wait_for(time_element.count(), timeout=2)
        if count > 0:
            datetime_attr = await asyncio.wait_for(time_element.get_attribute('datetime'), timeout=2)
            if datetime_attr:
                return datetime_attr
    except (asyncio.TimeoutError, Exception):
        pass
    
    try:
        time_element = tweet_element.locator('time').first
        count = await asyncio.wait_for(time_element.count(), timeout=2)
        if count > 0:
            title_attr = await asyncio.wait_for(time_element.get_attribute('title'), timeout=2)
            if title_attr:
                return title_attr
    except (asyncio.TimeoutError, Exception):
        pass
    
    try:
        time_element = tweet_element.locator('time').first
        count = await asyncio.wait_for(time_element.count(), timeout=2)
        if count > 0:
            time_text = await asyncio.wait_for(time_element.inner_text(), timeout=2)
            if time_text:
                return time_text.strip()
    except (asyncio.TimeoutError, Exception):
        pass
    
    try:
        timestamp_selectors = [
            'a[href*="/status/"] time',
            'time[datetime]',
            '[data-testid*="time"]',
            '[aria-label*="time"]',
            '[title*="AM"]',
            '[title*="PM"]'
        ]
        
        for selector in timestamp_selectors:
            try:
                element = tweet_element.locator(selector).first
                if await element.count() > 0:
                    datetime_attr = await element.get_attribute('datetime')
                    if datetime_attr:
                        return datetime_attr
                    
                    title_attr = await element.get_attribute('title')
                    if title_attr:
                        return title_attr
                    
                    text = await element.inner_text()
                    if text:
                        return text.strip()
            except Exception:
                continue
    except Exception:
        pass
    
    return ""

async def get_tweet_id(tweet_element) -> str:
    """Get a unique identifier for a tweet with improved robustness."""
    try:
        links = tweet_element.locator('a[href*="/status/"]')
        count = await links.count()
        for i in range(count):
            try:
                href = await links.nth(i).get_attribute('href')
                if href and "/status/" in href:
                    tweet_id = href.split('/status/')[-1].split('?')[0]
                    if tweet_id.isdigit() and len(tweet_id) > 10:
                        return tweet_id
            except Exception:
                continue
    except Exception:
        pass
    
    try:
        time_element = tweet_element.locator('time').first
        if await time_element.count() > 0:
            datetime_attr = await time_element.get_attribute('datetime')
            if datetime_attr:
                return f"time_{datetime_attr}"
    except Exception:
        pass
    
    try:
        data_attrs = ['data-tweet-id', 'data-testid', 'data-item-id']
        for attr in data_attrs:
            value = await tweet_element.get_attribute(attr)
            if value:
                return f"{attr}_{value}"
    except Exception:
        pass
    
    try:
        html_snippet = await tweet_element.inner_html()
        if html_snippet:
            content_hash = hashlib.md5(html_snippet[:1000].encode()).hexdigest()
            return f"hash_{content_hash}"
    except Exception:
        pass
    
    try:
        element_text = await tweet_element.inner_text()
        if element_text and len(element_text.strip()) > 5:
            text_snippet = element_text.strip()[:100]
            content_hash = hashlib.md5(text_snippet.encode()).hexdigest()[:12]
            return f"stable_{content_hash}"
    except Exception:
        pass
    
    return None

async def is_repost(tweet_element) -> bool:
    """Check if tweet is a repost (retweet without comment) with improved detection."""
    try:
        social_selectors = [
            'div[data-testid="socialContext"]',
            'div[data-testid="tweet"] div[data-testid="socialContext"]'
        ]
        
        for selector in social_selectors:
            try:
                social_context = tweet_element.locator(selector)
                if await social_context.count() > 0:
                    social_text = await social_context.inner_text()
                    retweet_indicators = ["reposted", "retweeted", "retweet", "shared"]
                    if any(indicator in social_text.lower() for indicator in retweet_indicators):
                        print(f"Found retweet via social context: {social_text}")
                        return True
            except Exception:
                continue

        retweet_indicators = [
            'div[data-testid="retweetIcon"]',
            'div[data-testid="retweet"]',
            'div[aria-label*="retweet" i]',
            'div[aria-label*="repost" i]',
            'svg[data-testid="icon-retweet"]'
        ]
        
        for indicator in retweet_indicators:
            try:
                element = tweet_element.locator(indicator)
                if await element.count() > 0:
                    print(f"Found retweet via indicator: {indicator}")
                    return True
            except Exception:
                continue

        try:
            article_text = await tweet_element.inner_text()
            if article_text:
                retweet_phrases = [
                    "retweeted", "reposted", "retweet", "shared this",
                    "reposted this", "retweeted this", "shared a"
                ]
                for phrase in retweet_phrases:
                    if phrase in article_text.lower():
                        print(f"Found retweet via text pattern: {phrase}")
                        return True
        except Exception:
            pass

        try:
            nested_selectors = [
                'div[data-testid="tweet"] div[data-testid="tweet"]',
                'article div[data-testid="tweet"]'
            ]
            
            for selector in nested_selectors:
                nested_tweet = tweet_element.locator(selector)
                if await nested_tweet.count() > 0:
                    print("Found retweet via nested structure")
                    return True
        except Exception:
            pass

    except Exception as e:
        print(f"Error checking repost status: {str(e)}")
    
    return False

async def is_quote_tweet(tweet_element) -> bool:
    """Check if tweet is a quote tweet"""
    try:
        quoted_tweet = tweet_element.locator('div[data-testid="tweet"]').last
        return await quoted_tweet.count() > 0
    except Exception:
        pass
    return False

async def get_retweet_info(tweet_element, page: Page) -> Optional[Dict[str, str]]:
    """Get information about a retweet WITHOUT bio fetching (to prevent hanging)."""
    try:
        main_content = await get_main_tweet_content(tweet_element)
        
        username = ""
        username_selectors = [
            'div[data-testid="User-Name"] a',
            'div[data-testid="User-Name"] span:has-text("@")',
            'a[href*="/"]:not([href*="/status/"])'
        ]
        
        for selector in username_selectors:
            try:
                elements = tweet_element.locator(selector)
                count = await asyncio.wait_for(elements.count(), timeout=3)
                
                for i in range(min(count, 5)):  # Limit to first 5 elements
                    try:
                        element = elements.nth(i)
                        
                        # Try to get username from href with timeout
                        try:
                            href = await asyncio.wait_for(element.get_attribute('href'), timeout=2)
                            if href and '/' in href and not '/status/' in href:
                                potential_username = href.strip('/').split('/')[-1]
                                if potential_username and not potential_username.startswith('http'):
                                    username = potential_username
                                    break
                        except (asyncio.TimeoutError, Exception):
                            pass
                        
                        # Try to get username from text content with timeout
                        try:
                            text = await asyncio.wait_for(element.inner_text(), timeout=2)
                            if text and '@' in text:
                                username = text.strip('@').split()[0]
                                break
                        except (asyncio.TimeoutError, Exception):
                            pass
                            
                    except Exception:
                        continue
                        
                if username:
                    break
            except (asyncio.TimeoutError, Exception) as e:
                print(f"Timeout/error in username extraction with selector {selector}: {str(e)}")
                continue

        bio = ""
        print(f"Skipped bio fetching for @{username} (disabled to prevent hanging)")

        return {
            "retweet_content": "",  
            "retweet_username": username,
            "retweet_profile_bio": bio, 
            "retweet_main_content": main_content
        }

    except Exception as e:
        print(f"Could not get retweet info: {str(e)}")
        return None

async def scrape_tweets(page: Page, username: str, max_tweets: int = 100, max_retweets: int = 100, stop_date=None) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Scrape tweets and retweets with improved efficiency and error handling."""
    tweets = []
    retweets = []
    processed_ids: Set[str] = set()
    
    # Initialize scroll_attempts at the beginning to avoid variable scope issues
    scroll_attempts = 0
    max_scroll_attempts = 50
    
    try:
        print(f"\nStarting to scrape tweets for user: {username} (max {max_tweets} tweets, {max_retweets} retweets)")
        if stop_date:
            print(f"🗓️  Will also stop when reaching tweets from {stop_date} or earlier")
        else:
            print("📅 No stop date set - will only stop at tweet limit or end of timeline")
        
        
        await page.goto(f"https://twitter.com/{username}", wait_until="domcontentloaded", timeout=TIMEOUT)
        
        if not await wait_for_profile_load(page, username):
            print("Profile could not be loaded")
            return tweets, retweets

        print("Profile loaded successfully")
        
        last_height = await page.evaluate("document.body.scrollHeight")
        no_new_items_count = 0
        max_no_new_items = 3 

        while scroll_attempts < max_scroll_attempts:
            scroll_attempts += 1
            
            try:
                # Wait for content to load
                await rate_limit_delay(SCROLL_DELAY)
                
                # Get all visible tweets with timeout protection
                try:
                    tweet_elements = await asyncio.wait_for(
                        page.locator('article[data-testid="tweet"]').all(),
                        timeout=5
                    )
                    print(f"[DEBUG] Found {len(tweet_elements)} tweet elements on scroll {scroll_attempts}")
                except (asyncio.TimeoutError, Exception) as e:
                    print(f"[DEBUG] Timeout/error getting tweet elements on scroll {scroll_attempts}: {str(e)}")
                    tweet_elements = []
                
                if not tweet_elements:
                    print(f"No tweets found on attempt {scroll_attempts}, waiting and retrying...")
                    await asyncio.sleep(3)  # Wait longer
                    
                    try:
                        await asyncio.wait_for(
                            page.evaluate("window.scrollBy(0, window.innerHeight * 0.8)"),
                            timeout=3
                        )
                        await asyncio.sleep(2)
                        
                        tweet_elements = await asyncio.wait_for(
                            page.locator('article[data-testid="tweet"]').all(),
                            timeout=5
                        )
                        print(f"[DEBUG] After gentle retry: Found {len(tweet_elements)} tweet elements")
                    except (asyncio.TimeoutError, Exception) as e:
                        print(f"[DEBUG] Retry scroll/detection timeout: {str(e)}")
                        tweet_elements = []
                    
                    if not tweet_elements:
                        print("Still no tweets found, but continuing...")
                        no_new_items_count += 1
                        if no_new_items_count >= max_no_new_items:
                            print("Multiple attempts failed, ending tweets scraping")
                            break
                        continue

                initial_count = len(tweets) + len(retweets)
                processed_in_batch = 0
                visible_tweet_ids = []  # Track tweets visible in current scroll

                for idx, tweet in enumerate(tweet_elements):
                    try:
                        try:
                            is_visible = await asyncio.wait_for(
                                tweet.is_visible(),
                                timeout=1
                            )
                            if not is_visible:
                                continue  # Skip invisible tweets
                        except:
                            pass  # Continue processing if visibility check fails
                        
                        # Get unique tweet ID
                        tweet_id = await get_tweet_id(tweet)
                        
                        # Skip elements with no valid ID (probably no useful content)
                        if tweet_id is None:
                            continue
                        
                        # Track this tweet as visible in current batch
                        visible_tweet_ids.append(tweet_id)
                        
                        if tweet_id in processed_ids:
                            continue
                        
                        processed_ids.add(tweet_id)
                        processed_in_batch += 1
                        
                        # Check if it's a retweet only if we want retweets
                        is_retweet = False
                        if max_retweets > 0:
                            is_retweet = await is_repost(tweet)
                        
                        if is_retweet and max_retweets > 0:
                            # Check retweet limit
                            if len(retweets) >= max_retweets:
                                print(f"Reached maximum retweets limit ({max_retweets}), skipping further retweets")
                                continue
                                
                            print(f"Processing retweet #{len(retweets)+1} (ID: {tweet_id})")
                            
                            try:
                                print(f"Getting date for retweet {tweet_id}...")
                                retweet_date = await asyncio.wait_for(
                                    get_tweet_date(tweet),
                                    timeout=3
                                )
                                print(f"Date extracted for retweet {tweet_id}: {retweet_date}")
                            except asyncio.TimeoutError:
                                print(f"Timeout getting date for retweet {tweet_id}")
                                retweet_date = "Unknown"
                            except Exception as e:
                                print(f"Error getting date for retweet {tweet_id}: {str(e)}")
                                retweet_date = "Unknown"

                            # Get retweet info with timeout and debugging
                            try:
                                print(f"Getting retweet info for {tweet_id}...")
                                retweet_info = await asyncio.wait_for(
                                    get_retweet_info(tweet, page),
                                    timeout=15  # Longer timeout as this includes bio fetching
                                )
                                print(f"Retweet info extracted for {tweet_id}")
                            except asyncio.TimeoutError:
                                print(f"Timeout getting retweet info for {tweet_id}")
                                retweet_info = None
                            except Exception as e:
                                print(f"Error getting retweet info for {tweet_id}: {str(e)}")
                                retweet_info = None
                            
                            if retweet_info:
                                retweet_info["retweet_date"] = retweet_date
                                retweets.append(retweet_info)
                                print(f"Successfully added retweet {len(retweets)} with date: {retweet_date}")
                            else:
                                print(f"Could not extract retweet info for {tweet_id}")
                            continue

                        if len(tweets) >= max_tweets:
                            print(f"Reached maximum tweets limit ({max_tweets}), skipping further tweets")
                            continue

                        print(f"Processing tweet #{len(tweets)+1} (ID: {tweet_id})")
                        
                        try:
                            print(f"Getting content for tweet {tweet_id}...")
                            content = await asyncio.wait_for(
                                get_main_tweet_content(tweet),
                                timeout=5
                            )
                            print(f"Content extracted for tweet {tweet_id}: {len(content) if content else 0} chars")
                        except asyncio.TimeoutError:
                            print(f"Timeout getting content for tweet {tweet_id}")
                            content = None
                        except Exception as e:
                            print(f"Error getting content for tweet {tweet_id}: {str(e)}")
                            content = None
                        
                        if content:
                            try:
                                print(f"Getting date for tweet {tweet_id}...")
                                tweet_date = await asyncio.wait_for(
                                    get_tweet_date(tweet),
                                    timeout=3
                                )
                                print(f"Date extracted for tweet {tweet_id}: {tweet_date}")
                            except asyncio.TimeoutError:
                                print(f"Timeout getting date for tweet {tweet_id}")
                                tweet_date = "Unknown"
                            except Exception as e:
                                print(f"Error getting date for tweet {tweet_id}: {str(e)}")
                                tweet_date = "Unknown"
                            
                            if should_stop_at_date(tweet_date, stop_date):
                                print(f"Stopping scraping with {len(tweets)} tweets collected.")
                                return tweets, retweets
                            
                            tweet_data = {
                                "tweet_content": content,
                                "tweet_date": tweet_date
                            }

                            try:
                                quoted_info = await get_quoted_tweet_info(tweet)
                                if quoted_info:
                                    tweet_data.update(quoted_info)
                            except Exception as e:
                                print(f"Error getting quoted tweet info: {str(e)}")

                            tweets.append(tweet_data)
                            print(f"Successfully added tweet {len(tweets)} with date: {tweet_date}")
                        else:
                            print(f"Tweet {tweet_id} has no detectable content, skipping")

                    except Exception as e:
                        print(f"Error processing tweet element: {str(e)}")
                        continue

                current_count = len(tweets) + len(retweets)
                print(f"Batch {scroll_attempts}: Processed {processed_in_batch} new items. Total: {len(tweets)} tweets, {len(retweets)} retweets")
                print(f"[DEBUG] Visible tweets in this batch: {len(visible_tweet_ids)}, New processed: {processed_in_batch}")
                
                if len(visible_tweet_ids) > processed_in_batch + 3:  
                    print(f"[WARNING] High overlap detected: {len(visible_tweet_ids)} visible, {processed_in_batch} new. May indicate fast scrolling.")
                
                tweets_limit_reached = len(tweets) >= max_tweets
                retweets_limit_reached = (max_retweets == 0) or (len(retweets) >= max_retweets)
                
                if tweets_limit_reached and retweets_limit_reached:
                    if max_retweets > 0:
                        print(f"Reached both limits: {len(tweets)} tweets (max: {max_tweets}), {len(retweets)} retweets (max: {max_retweets})")
                    else:
                        print(f"Reached tweet limit: {len(tweets)} tweets (max: {max_tweets}), retweets disabled")
                    break
                
                if current_count == initial_count:
                    no_new_items_count += 1
                    print(f"No new items found (attempt {no_new_items_count}/{max_no_new_items})")
                    
                    if len(visible_tweet_ids) > 5:  # Many tweets visible but none new
                        print(f"[DEBUG] Many tweets visible ({len(visible_tweet_ids)}) but none new - possible fast scrolling issue")
                        # Wait a bit longer for content to settle
                        await asyncio.sleep(1)
                else:
                    no_new_items_count = 0

                if no_new_items_count >= max_no_new_items:
                    print("Reached end of timeline (no new items)")
                    break

                print(f"Scrolling for more content... (attempt {scroll_attempts})")
                
                try:
                    current_scroll = await asyncio.wait_for(
                        page.evaluate("window.pageYOffset"),
                        timeout=3
                    )
                    
                    scroll_distance = min(
                        await asyncio.wait_for(page.evaluate("window.innerHeight"), timeout=3),
                        800
                    )
                    
                    for i in range(3):
                        await asyncio.wait_for(
                            page.evaluate(f"window.scrollBy(0, {scroll_distance // 3})"),
                            timeout=3
                        )
                        await asyncio.sleep(0.5) 
                        
                        try:
                            new_tweet_count = await asyncio.wait_for(
                                page.locator('article[data-testid="tweet"]').count(),
                                timeout=2
                            )
                            if new_tweet_count > len(tweet_elements):
                                print(f"[DEBUG] New tweets loaded during micro-scroll {i+1}: {new_tweet_count} vs {len(tweet_elements)}")
                                break  
                        except:
                            pass
                    
                    if processed_in_batch > 0:
                        await asyncio.sleep(2.0)  # Longer wait when successfully collecting
                        print(f"[DEBUG] Extended wait due to successful collection ({processed_in_batch} new items)")
                    else:
                        # Standard wait when not finding much
                        await asyncio.sleep(1.5)
                    
                    # Check if we actually scrolled
                    new_scroll = await asyncio.wait_for(
                        page.evaluate("window.pageYOffset"),
                        timeout=3
                    )
                    
                    if abs(new_scroll - current_scroll) < 50:
                        print("[DEBUG] Small scroll detected, trying alternative method")
                        try:
                            await asyncio.wait_for(page.keyboard.press('PageDown'), timeout=2)
                            await asyncio.sleep(1)
                        except (asyncio.TimeoutError, Exception):
                            pass
                    
                except (asyncio.TimeoutError, Exception) as e:
                    print(f"Scroll operation timeout/error: {str(e)}")
                
                await rate_limit_delay(SCROLL_DELAY)

                try:
                    new_height = await asyncio.wait_for(
                        page.evaluate("document.body.scrollHeight"),
                        timeout=3
                    )
                except (asyncio.TimeoutError, Exception) as e:
                    print(f"Page height check timeout: {str(e)}")
                    new_height = last_height  # Assume no change
                
                if new_height == last_height:
                    try:
                        await asyncio.wait_for(
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight + 1000)"),
                            timeout=3
                        )
                        await asyncio.sleep(2)
                        new_height = await asyncio.wait_for(
                            page.evaluate("document.body.scrollHeight"),
                            timeout=3
                        )
                    except (asyncio.TimeoutError, Exception) as e:
                        print(f"Aggressive scroll timeout: {str(e)}")
                        new_height = last_height
                    
                    if new_height == last_height:
                        no_new_items_count += 1
                        print("Page height unchanged, may have reached end")
                    else:
                        last_height = new_height
                        print("Additional scroll worked, continuing...")
                else:
                    last_height = new_height

            except Exception as e:
                print(f"Error during scroll {scroll_attempts}: {str(e)}")
                no_new_items_count += 1

            if no_new_items_count >= max_no_new_items:
                print("Stopping due to no new items")
                break

    except Exception as e:
        print(f"Error scraping tweets: {str(e)}")
        if 'scroll_attempts' not in locals():
            scroll_attempts = 0

    print(f"\nScraping completed after {scroll_attempts} scroll attempts!")
    print(f"Final results: {len(tweets)} tweets and {len(retweets)} retweets")
    return tweets, retweets

async def scrape_likes(page, username: str) -> List[Dict]:
    return []

async def scrape_social_users(page: Page, username: str, user_type: str, max_users: int = 300) -> List[Dict[str, str]]:
    """Generic function to scrape followers or following with improved efficiency."""
    users = []
    try:
        url = f"https://twitter.com/{username}/{user_type}"
        print(f"Navigating to {url} (max: {max_users} users)")
        await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
        await rate_limit_delay()

        if not await safe_wait_for_selector(page, 'div[data-testid="cellInnerDiv"]', timeout=3000, description=f"{user_type} cells"):
            print(f"No {user_type} cells found")
            return users

        processed_usernames: Set[str] = set()
        no_new_users_count = 0
        max_no_new_users = 5  
        scroll_attempts = 0
        max_scroll_attempts = 30

        while scroll_attempts < max_scroll_attempts:
            scroll_attempts += 1
            try:
                cells = await page.locator('div[data-testid="cellInnerDiv"]').all()
                
                if not cells:
                    no_new_users_count += 1
                    if no_new_users_count >= max_no_new_users:
                        print(f"No more {user_type} cells found after multiple attempts")
                        break
                    await rate_limit_delay()
                    continue

                initial_count = len(users)
                processed_in_batch = 0

                for cell in cells:
                    try:
                        cell_username = await extract_username_from_cell(cell)
                        if not cell_username or cell_username in processed_usernames:
                            continue
                        
                        if len(users) >= max_users:
                            print(f"Reached maximum {user_type} limit ({max_users}), stopping collection")
                            break
                            
                        processed_usernames.add(cell_username)
                        processed_in_batch += 1
                        
                        display_name = await extract_display_name_from_cell(cell, cell_username)
                        
                        bio = await extract_bio_from_cell(cell)
                        
                        if user_type == "followers":
                            user_data = {
                                "follower_name": display_name or cell_username,
                                "follower_bio": bio
                            }
                        else: 
                            user_data = {
                                "following_name": display_name or cell_username,
                                "following_bio": bio
                            }
                        
                        users.append(user_data)
                        print(f"Added {user_type[:-1]} #{len(users)}: @{cell_username}" + (f" ({display_name})" if display_name else ""))
                        
                        if bio:
                            print(f"  Bio: {bio[:50]}..." if len(bio) > 50 else f"  Bio: {bio}")
                        
                    except Exception as e:
                        print(f"Error processing {user_type} cell: {str(e)}")
                        continue

                # Check progress and limits
                current_count = len(users)
                print(f"Batch {scroll_attempts}: Processed {processed_in_batch} new {user_type}. Total: {current_count}")
                
                # Check if we've reached the user limit
                if len(users) >= max_users:
                    print(f"Reached maximum {user_type} limit ({max_users}), stopping")
                    break
                
                if current_count == initial_count:
                    no_new_users_count += 1
                    print(f"No new {user_type} found (attempt {no_new_users_count}/{max_no_new_users})")
                else:
                    no_new_users_count = 0

                if no_new_users_count >= max_no_new_users:
                    print(f"Reached end of {user_type} list")
                    break

                # Scroll down
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await rate_limit_delay(SCROLL_DELAY)

            except Exception as e:
                print(f"Error during {user_type} scroll {scroll_attempts}: {str(e)}")
                no_new_users_count += 1

        print(f"Total {user_type} collected: {len(users)}")
        return users

    except Exception as e:
        print(f"Error scraping {user_type}: {str(e)}")
        return users

async def extract_username_from_cell(cell) -> str:
    """Extract username from a user cell."""
    username_selectors = [
        'a[role="link"][href*="/"]',
        'a[href*="/"]',
        'div[data-testid="User-Name"] a'
    ]
    
    for selector in username_selectors:
        try:
            links = cell.locator(selector)
            count = await links.count()
            for i in range(count):
                href = await links.nth(i).get_attribute('href')
                if href and '/' in href and not '/status/' in href:
                    potential_username = href.strip('/').split('/')[-1]
                    if potential_username and not potential_username.startswith('http'):
                        return potential_username
        except Exception:
            continue
    
    return ""

async def extract_display_name_from_cell(cell, username: str) -> str:
    """Extract display name from a user cell."""
    name_selectors = [
        'div[data-testid="User-Name"] div:first-child span span',
        'div[data-testid="User-Name"] div span',
        'div[data-testid="User-Name"] span'
    ]
    
    for selector in name_selectors:
        try:
            name_element = cell.locator(selector).first
            if await name_element.count() > 0:
                name = await name_element.inner_text()
                if name and name.strip() and not name.startswith('@'):
                    # Clean up name
                    name = name.strip().replace("·", "").strip()
                    if name != username:  # Don't return username as display name
                        return name
        except Exception:
            continue
    
    return ""

async def extract_bio_from_cell(cell) -> str:
    """Extract bio from a user cell."""
    bio_selectors = [
        'div[data-testid="UserDescription"]',
        'div[data-testid="UserBio"]',
        'div[data-testid="UserProfessionalCategory"]'
    ]
    
    for selector in bio_selectors:
        try:
            bio_element = cell.locator(selector)
            if await bio_element.count() > 0:
                bio = await bio_element.inner_text()
                if bio:
                    return bio.strip().replace("Follow", "").strip()
        except Exception:
            continue
    
    # Fallback: try to find any descriptive text
    try:
        text_elements = cell.locator('div[dir="auto"]')
        count = await text_elements.count()
        for i in range(count):
            text = await text_elements.nth(i).inner_text()
            if text and len(text) > 5 and not text.startswith('@') and 'Follow' not in text:
                return text.strip()
    except Exception:
        pass
    
    return ""

async def scrape_followers(page: Page, username: str, max_followers: int = 300) -> List[Dict[str, str]]:
    """Scrape followers using the generic social scraping function."""
    return await scrape_social_users(page, username, "followers", max_followers)

async def scrape_following(page: Page, username: str, max_following: int = 300) -> List[Dict[str, str]]:
    """Scrape following using the generic social scraping function."""
    return await scrape_social_users(page, username, "following", max_following)

async def scrape_retweets(page, username: str, max_retweets: int = 100) -> List[Dict]:
    retweets = []
    try:
        print(f"Starting to scrape retweets for {username} (max: {max_retweets})")
        # Navigate to profile with better error handling
        try:
            await page.goto(f"https://twitter.com/{username}", wait_until="domcontentloaded", timeout=30000)
            if not await wait_for_profile_load(page, username):
                return retweets
                
        except Exception as e:
            print(f"Error accessing profile {username}: {str(e)}")
            return retweets
        
        last_height = await page.evaluate("document.body.scrollHeight")
        processed_retweets = set()
        no_new_retweets_count = 0
        max_no_new_retweets = 3  # Restored from 2 to 3
        
        while True:
            try:
                # Get all visible tweets with timeout
                tweet_elements = await page.locator('article[data-testid="tweet"]').all()
                if not tweet_elements:
                    print("No tweets found on page")
                    break
                    
                initial_retweet_count = len(retweets)
                
                for tweet in tweet_elements:
                    try:
                        # Get tweet ID or some unique identifier
                        tweet_html = await tweet.inner_html()
                        tweet_hash = hash(tweet_html)
                        
                        if tweet_hash in processed_retweets:
                            continue
                            
                        processed_retweets.add(tweet_hash)
                        
                        # Only process pure reposts (no comment)
                        if not await is_repost(tweet):
                            continue
                        
                        # Check if we've reached the retweet limit
                        if len(retweets) >= max_retweets:
                            print(f"Reached maximum retweets limit ({max_retweets}), stopping retweet collection")
                            break
                        
                        # Get retweet data
                        original_content = await get_main_tweet_content(tweet)
                        retweeted_username = ""
                        retweeted_bio = ""
                        
                        # Get retweet date
                        retweet_date = await get_tweet_date(tweet)
                        
                        # Get username of retweeted content
                        try:
                            name_element = tweet.locator('div[data-testid="User-Name"] a').first
                            if await name_element.count() > 0:
                                retweeted_username = await name_element.get_attribute('href')
                                if retweeted_username:
                                    retweeted_username = retweeted_username.replace('/', '')
                        except Exception as e:
                            print(f"Could not get username: {str(e)}")
                        
                        # Get screenshot with unique filename - DISABLED
                        try:
                            if original_content or retweeted_username:
                                retweets.append({
                                    "retweet_content": "",  # Retweet itself has no content
                                    "retweet_username": retweeted_username,
                                    "retweet_profile_bio": retweeted_bio,
                                    "retweet_date": retweet_date,
                                    "retweet_main_content": original_content
                                })
                                print(f"Added retweet {len(retweets)} from {retweeted_username} with date: {retweet_date}")
                        except Exception as e:
                            print(f"Could not process retweet: {str(e)}")
                    
                    except Exception as e:
                        print(f"Error processing retweet: {str(e)}")
                        continue
                
                # Check if we found any new retweets or reached limit
                if len(retweets) >= max_retweets:
                    print(f"Reached maximum retweets limit ({max_retweets}), stopping")
                    break
                    
                if len(retweets) == initial_retweet_count:
                    no_new_retweets_count += 1
                else:
                    no_new_retweets_count = 0
                    print(f"Found {len(retweets)} retweets so far")
                
                # Stop if we haven't found new retweets in several scrolls
                if no_new_retweets_count >= max_no_new_retweets:
                    print("No new retweets found after multiple scrolls, stopping")
                    break
                
                # Scroll down with timeout
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)
                    
                    # Check if we've reached the bottom
                    new_height = await page.evaluate("document.body.scrollHeight")
                    if new_height == last_height:
                        no_new_retweets_count += 1
                    else:
                        last_height = new_height
                        
                except Exception as e:
                    print(f"Error scrolling: {str(e)}")
                    no_new_retweets_count += 1
                
            except Exception as e:
                print(f"Error during retweet collection: {str(e)}")
                no_new_retweets_count += 1
            
            # Emergency break if something goes wrong
            if no_new_retweets_count >= max_no_new_retweets:
                break
                
    except Exception as e:
        print(f"Error scraping retweets: {str(e)}")
    
    print(f"Total retweets scraped: {len(retweets)}")
    return retweets

async def scrape_twitter(username: str, max_tweets: int = 100, max_retweets: int = 100, max_followers: int = 1000, max_following: int = 1000, stop_date=None) -> Dict:
    result = {
        "user_profile": {"username": username, "bio": ""},
        "following": [],
        "followers": []
    }
    
    try:
        async with async_playwright() as p:
            # Detect if we have a display available
            has_display = os.environ.get('DISPLAY') is not None
            
            # Launch browser with appropriate settings
            launch_args = [
                '--disable-extensions',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-gpu' if not has_display else '',
            ]
            # Remove empty strings
            launch_args = [arg for arg in launch_args if arg]
            
            print(f"🖥️  Display available: {has_display} (DISPLAY={os.environ.get('DISPLAY', 'None')})")
            print(f"🚀 Browser args: {launch_args}")
            
            browser = await p.chromium.launch(
                headless=not has_display,
                args=launch_args
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            )
            
            if os.path.exists(COOKIES_FILE):
                try:
                    with open(COOKIES_FILE, "r") as f:
                        cookies = json.load(f)
                    await context.add_cookies(cookies)
                    print("Cookies loaded successfully")
                except Exception as e:
                    print(f"Error loading cookies: {str(e)}")
                    await browser.close()
                    return result
            else:
                print("No cookies file found. Please run login_manual.py first")
                await browser.close()
                return result
            
            try:
                page = await context.new_page()
                page.set_default_timeout(60000) 
                
                print("\nAccessing Twitter...")
                try:
                    await page.goto("https://twitter.com", wait_until="domcontentloaded")
                except Exception as e:
                    print(f"Error accessing Twitter: {str(e)}")
                    await browser.close()
                    return result

                # Short wait for initial load
                await asyncio.sleep(1)
                
                # Check for login state using multiple indicators
                print("Verifying login status...")
                try:
                    # Wait a bit longer for the page to load completely
                    await asyncio.sleep(1)
                    
                    login_verified = False
                    
                    try:
                        login_button = page.locator('a[href="/login"]')
                        if await login_button.count() > 0:
                            print("Not logged in (login button found). Please run login_manual.py again.")
                            await browser.close()
                            return result
                    except Exception as e:
                        print(f"Could not check for login button: {str(e)}")
                    
                    try:
                        signup_button = page.locator('a[href="/i/flow/signup"]')
                        if await signup_button.count() > 0:
                            print("Not logged in (signup button found). Please run login_manual.py again.")
                            await browser.close()
                            return result
                    except Exception as e:
                        print(f"Could not check for signup button: {str(e)}")
                    
                    try:
                        timeline = page.locator('div[data-testid="primaryColumn"]')
                        if await timeline.count() > 0:
                            print("Login verified - timeline found")
                            login_verified = True
                    except Exception as e:
                        print(f"Could not check for timeline: {str(e)}")
                    
                    if not login_verified:
                        try:
                            profile_link = page.locator('a[data-testid="AppTabBar_Profile_Link"]')
                            if await profile_link.count() > 0:
                                print("Login verified - profile link found")
                                login_verified = True
                        except Exception as e:
                            print(f"Could not check for profile link: {str(e)}")
                    
                    if not login_verified:
                        try:
                            authenticated_selectors = [
                                'div[data-testid="SideNav_AccountSwitcher_Button"]',
                                'div[data-testid="AppTabBar_Home_Link"]',
                                'div[data-testid="AppTabBar_Explore_Link"]',
                                'div[data-testid="AppTabBar_Notifications_Link"]'
                            ]
                            
                            for selector in authenticated_selectors:
                                element = page.locator(selector)
                                if await element.count() > 0:
                                    print(f"Login verified - authenticated element found: {selector}")
                                    login_verified = True
                                    break
                        except Exception as e:
                            print(f"Could not check for authenticated elements: {str(e)}")
                    
                    if not login_verified:
                        try:
                            current_url = page.url
                            if "twitter.com/home" in current_url or "x.com/home" in current_url:
                                print("Login verified - on home page")
                                login_verified = True
                        except Exception as e:
                            print(f"Could not check URL: {str(e)}")
                    
                    if not login_verified:
                        print("Could not verify login status with any method.")
                        print("This might be due to Twitter's anti-bot measures or page loading issues.")
                        print("Attempting to continue anyway...")
                    else:
                        print("Login verified successfully")

                except Exception as e:
                    print(f"Error during login verification: {str(e)}")
                    print("Attempting to continue anyway...")

                print(f"\nNavigating to profile @{username}...")
                navigation_success = False
                for attempt in range(3): 
                    try:
                        await page.goto(f"https://twitter.com/{username}", wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(2)  
                        navigation_success = True
                        break
                    except Exception as e:
                        print(f"Navigation attempt {attempt + 1}/3 failed: {str(e)}")
                        if attempt < 2:  # Not the last attempt
                            print("Retrying navigation...")
                            await asyncio.sleep(5)  # Wait before retrying
                        
                if not navigation_success:
                    print(f"Error navigating to profile after 3 attempts")
                    await browser.close()
                    return result
                
                # Verify profile exists and is accessible
                try:
                    # Check for error messages
                    error_selectors = [
                        'div[data-testid="error-detail"]',
                        'div[data-testid="empty-state"]',
                        'div[data-testid="404-error"]'
                    ]
                    
                    for selector in error_selectors:
                        error_element = page.locator(selector)
                        if await error_element.count() > 0:
                            error_text = await error_element.inner_text()
                            print(f"Profile error: {error_text}")
                            await browser.close()
                            return result
                            
                    # Verify profile content is visible with retry logic
                    profile_accessed = False
                    for attempt in range(3):
                        try:
                            # Try multiple selectors to detect profile
                            profile_selectors = [
                                'div[data-testid="UserName"]',
                                'h2[aria-level="2"]',
                                'div[data-testid="UserDescription"]',
                                'article[data-testid="tweet"]',
                                'div[data-testid="primaryColumn"]'
                            ]
                            
                            for selector in profile_selectors:
                                try:
                                    await page.wait_for_selector(selector, timeout=3000)
                                    profile_accessed = True
                                    print(f"✅ Profile @{username} accessed successfully (detected via {selector})")
                                    break
                                except:
                                    continue
                            
                            if profile_accessed:
                                break
                            else:
                                if attempt < 2:
                                    print(f"Profile detection attempt {attempt + 1}/3 failed, retrying...")
                                    await asyncio.sleep(2)
                        except Exception as e:
                            if attempt < 2:
                                print(f"Profile verification attempt {attempt + 1}/3 failed: {e}, retrying...")
                                await asyncio.sleep(2)
                    
                        
                except Exception as e:
                    print(f"Error verifying profile: {str(e)}")
                    await browser.close()
                    return result

                # Get profile info
                print(f"Fetching profile info for @{username}...")
                result["user_profile"] = await scrape_user_profile(page, username)
                print(f"Profile info fetched: {result['user_profile']}")
                
                if not result["user_profile"]["bio"] and not result["user_profile"]["username"]:
                    print(f"Could not fetch profile info for @{username}")
                    await browser.close()
                    return result
                
                # Get tweets and retweets
                print(f"\nFetching tweets and retweets for @{username}...")
                tweets, retweets = await scrape_tweets(page, username, max_tweets, max_retweets, stop_date)
                if tweets:
                    result["tweets"] = tweets
                    print(f"Found {len(tweets)} tweets")
                else:
                    print("No tweets found or error occurred")
                    
                if retweets:
                    result["retweets"] = retweets
                    print(f"Found {len(retweets)} retweets")
                else:
                    print("No retweets found or error occurred")
                
                if max_followers > 0 or max_following > 0:
                    social_page = await context.new_page()
                    social_page.set_default_timeout(60000)
                    
                    if max_followers > 0:
                        print(f"\nFetching followers for @{username}...")
                        followers = await scrape_followers(social_page, username, max_followers)
                        if followers:
                            result["followers"] = followers
                            print(f"Found {len(followers)} followers")
                        else:
                            print("No followers found or error occurred")
                    else:
                        print(f"\nSkipping followers (limit set to 0)")
                    
                    # Small delay between operations
                    await asyncio.sleep(1)
                    
                    if max_following > 0:
                        print(f"\nFetching following for @{username}...")
                        following = await scrape_following(social_page, username, max_following)
                        if following:
                            result["following"] = following
                            print(f"Found {len(following)} following")
                        else:
                            print("No following found or error occurred")
                    else:
                        print(f"\nSkipping following (limit set to 0)")
                    
                    await social_page.close()
                else:
                    print(f"\nSkipping followers and following (both limits set to 0)")
                
                # Scraping completed
                print("\nScraping completed successfully!")
                
            except Exception as e:
                print(f"Error during scraping: {str(e)}")
            finally:
                print("\nClosing browser...")
                await safe_browser_close(browser)
                    
    except Exception as e:
        print(f"Critical error: {str(e)}")
    
    try:
        scraped_profiles_dir = os.path.join(os.path.dirname(__file__), '..', 'scraped_profiles')
        os.makedirs(scraped_profiles_dir, exist_ok=True)
        json_path = os.path.join(scraped_profiles_dir, f"{username}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(result['tweets'], f, ensure_ascii=False, indent=2)
        print(f"Scraped profile saved to {json_path}")
    except Exception as e:
        print(f"Error saving scraped profile: {str(e)}")
    
    return result
