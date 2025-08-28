from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time

def scroll_spotify_discography():
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Keep browser visible to see the human-like scrolling
    
    # Initialize the webdriver
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Open the Spotify URL
        url = "https://open.spotify.com/artist/5VVN3xZw1i2qihfITZlvCZ/discography/all"
        print(f"Opening URL: {url}")
        driver.get(url)
        
        # Wait for the page to load
        time.sleep(5)
        
        # Wait for the scrollable content area to be present
        wait = WebDriverWait(driver, 20)
        
        # Find the main content area (right side content) - Updated selectors
        try:
            # Based on the DOM structure visible in the screenshot
            selectors_to_try = [
                # Target the main content container
                '[data-overlayscrollbars-viewport]',
                '.os-viewport',
                # Target elements that have os-scrollbar classes nearby
                'div[class*="os-scrollbar"]',
                # Main content area
                'main[role="main"]',
                'main',
                # Content sections
                'section[data-testid*="discography"]',
                '[data-testid="artist-page"]',
                # General content containers
                'div[style*="overflow"]',
                '[class*="scroll"]'
            ]
            
            viewport = None
            for selector in selectors_to_try:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"Found {len(elements)} elements with selector: {selector}")
                    
                    for i, element in enumerate(elements):
                        try:
                            # Check if element is scrollable and has reasonable size
                            scroll_height = driver.execute_script("return arguments[0].scrollHeight", element)
                            client_height = driver.execute_script("return arguments[0].clientHeight", element)
                            
                            print(f"  Element {i}: ScrollHeight={scroll_height}, ClientHeight={client_height}")
                            
                            if scroll_height > client_height and client_height > 300:
                                viewport = element
                                class_name = element.get_attribute("class") or "no-class"
                                print(f"✓ Found scrollable viewport: {selector}")
                                print(f"  Class: {class_name}")
                                print(f"  Dimensions: {scroll_height}x{client_height}")
                                break
                        except Exception as e:
                            print(f"  Error checking element {i}: {e}")
                            continue
                    
                    if viewport:
                        break
                        
                except Exception as e:
                    print(f"Error with selector {selector}: {e}")
                    continue
            
            # If still not found, try to find the element that contains the discography content
            if not viewport:
                print("Trying to find discography container...")
                try:
                    # Look for elements containing album/song information
                    discography_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'songs') or contains(text(), 'album') or contains(text(), 'EP')]")
                    for elem in discography_elements:
                        parent = elem.find_element(By.XPATH, "..")
                        while parent:
                            try:
                                scroll_height = driver.execute_script("return arguments[0].scrollHeight", parent)
                                client_height = driver.execute_script("return arguments[0].clientHeight", parent)
                                if scroll_height > client_height and client_height > 300:
                                    viewport = parent
                                    print(f"Found viewport via discography content search")
                                    break
                                parent = parent.find_element(By.XPATH, "..")
                            except:
                                break
                        if viewport:
                            break
                except Exception as e:
                    print(f"Error in discography search: {e}")
            
            if not viewport:
                print("Still couldn't find scrollable content!")
                return
                
        except Exception as e:
            print(f"Error finding viewport: {str(e)}")
            return
        
        # Get initial scroll position and height
        initial_scroll_top = driver.execute_script("return arguments[0].scrollTop", viewport)
        initial_scroll_height = driver.execute_script("return arguments[0].scrollHeight", viewport)
        client_height = driver.execute_script("return arguments[0].clientHeight", viewport)
        
        print(f"Initial scroll position: {initial_scroll_top}")
        print(f"Initial scroll height: {initial_scroll_height}")
        print(f"Client height: {client_height}")
        
        # Continuous scrolling parameters
        scroll_step = 150  # Small scroll steps for smooth scrolling
        scroll_delay = 0.3  # Fast continuous scrolling (300ms between scrolls)
        
        scroll_count = 0
        max_scrolls = 1000  # Higher safety limit
        last_height = initial_scroll_height
        no_new_content_count = 0
        
        print("Starting continuous scrolling...")
        
        while scroll_count < max_scrolls:
            # Get current scroll position
            current_scroll_top = driver.execute_script("return arguments[0].scrollTop", viewport)
            current_height = driver.execute_script("return arguments[0].scrollHeight", viewport)
            max_scroll_top = current_height - client_height
            
            # Check if we've reached the bottom
            if current_scroll_top >= max_scroll_top - 20:  # 20px tolerance
                if current_height == last_height:
                    no_new_content_count += 1
                    if no_new_content_count >= 5:  # No new content for 5 checks
                        print("✓ Reached the bottom - no more content!")
                        break
                else:
                    no_new_content_count = 0  # Reset counter if new content appeared
                    last_height = current_height
            
            # Calculate next scroll position
            next_scroll_top = min(current_scroll_top + scroll_step, max_scroll_top)
            
            # Perform the scroll
            driver.execute_script("arguments[0].scrollTop = arguments[1]", viewport, next_scroll_top)
            
            scroll_count += 1
            if scroll_count % 20 == 0:  # Show progress every 20 scrolls to avoid spam
                print(f"Scroll #{scroll_count}: Position {next_scroll_top}px (Height: {current_height}px)")
            
            # Fast continuous scrolling
            time.sleep(scroll_delay)
        
        if scroll_count >= max_scrolls:
            print(f"⚠ Reached maximum scroll limit ({max_scrolls})")
        
        # Final position info
        final_scroll_top = driver.execute_script("return arguments[0].scrollTop", viewport)
        final_height = driver.execute_script("return arguments[0].scrollHeight", viewport)
        print(f"\n📊 Scrolling Summary:")
        print(f"   Total scrolls: {scroll_count}")
        print(f"   Initial height: {initial_scroll_height}px")
        print(f"   Final height: {final_height}px")
        print(f"   Final position: {final_scroll_top}px")
        print(f"   Content loaded: {final_height - initial_scroll_height}px")
        
        print("\n✅ Continuous scrolling completed!")
        print("Keeping browser open for 5 seconds...")
        time.sleep(5)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Close the browser
        driver.quit()
        print("Browser closed.")

if __name__ == "__main__":
    print("🎵 Spotify Discography Human-Like Auto-Scroller")
    print("=" * 50)
    scroll_spotify_discography()