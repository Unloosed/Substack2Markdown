EMAIL = "your-email@domain.com"
PASSWORD = "your-password"

from typing import List

# Moved from substack_scraper.py
USE_PREMIUM: bool = False  # Set to True if you want to login to Substack and convert paid for posts
SUBSTACK_URLS: List[str] = ["https://www.thefitzwilliam.com/", "https://www.citationneeded.news/"]  # Substack(s) you want to convert to markdown
BASE_MD_DIR: str = "substack_md_files"  # Name of the directory we'll save the .md essay files
BASE_HTML_DIR: str = "substack_html_pages"  # Name of the directory we'll save the .html essay files
HTML_TEMPLATE: str = "author_template.html"  # HTML template to use for the author page
JSON_DATA_DIR: str = "data"
NUM_POSTS_TO_SCRAPE: int = 3  # Set to 0 if you want all posts
