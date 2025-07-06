import argparse
import json
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from time import sleep
from datetime import datetime

from bs4 import BeautifulSoup
import html2text
import markdown
import requests
from tqdm import tqdm
from xml.etree import ElementTree as ET

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.service import Service
from urllib.parse import urlparse
from config import (
    EMAIL, PASSWORD, USE_PREMIUM, SUBSTACK_URLS, BASE_MD_DIR, BASE_EPUB_DIR,
    BASE_HTML_DIR, HTML_TEMPLATE, JSON_DATA_DIR, NUM_POSTS_TO_SCRAPE, DELAY_LENGTH
)
from ebooklib import epub
import shutil  # For robustly creating directories

def extract_main_part(url: str) -> str:
    parts = urlparse(url).netloc.split('.')  # Parse the URL to get the netloc, and split on '.'
    return parts[1] if parts[0] == 'www' else parts[0]  # Return the main part of the domain, while ignoring 'www' if
    # present


def format_substack_date(date_str: str) -> str:
    """
    Converts Substack date string (e.g., "Jan 1, 2023", "1 day ago", "1 hr ago") to YYYY-MM-DD format.
    Returns original string if parsing fails.
    """
    if date_str == "Date not found":
        return "Unknown Date"  # Or handle as an error, or return None

    date_str = date_str.strip() # Ensure all parsing attempts work with a clean string

    # Normalize common variations like "hours" to "hr"
    date_str = date_str.replace(" hours", " hr").replace(" hour", " hr")
    date_str = date_str.replace(" days", " day").replace(" day", " day")  # "1 day ago" is fine
    date_str = date_str.replace(" minutes", " min").replace(" minute", " min")

    # Define a mapping for English month abbreviations to ensure parsing works independently of locale
    month_abbr_map = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
        "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
    }

    try:
        # Attempt 1: Standard "%b %d, %Y" (e.g., "Jan 1, 2023", "Jun 09, 2025")
        # This is kept as it's the most direct if locale matches.
        dt_object = datetime.strptime(date_str, "%b %d, %Y")
        return dt_object.strftime("%Y-%m-%d")
    except ValueError:
        # Attempt 2: Handle "MMM DD, YYYY" with manual month replacement for locale independence
        # Example: "Jun 09, 2025"
        try:
            # date_str is already stripped from the function's start
            processed_date_str = date_str.replace(',', '') # Remove commas
            parts = processed_date_str.split() # Split by whitespace

            if len(parts) == 3:
                month_str = parts[0].strip()
                day_str = parts[1].strip()
                year_str = parts[2].strip()

                if month_str in month_abbr_map and day_str.isdigit() and year_str.isdigit():
                    month_num = month_abbr_map[month_str]
                    # strptime can handle single or double digit days with %d, and full year with %Y.
                    # No need to manually zfill day_str if using %d.
                    dt_object = datetime.strptime(f"{month_num} {day_str} {year_str}", "%m %d %Y")
                    return dt_object.strftime("%Y-%m-%d")
            # If parsing fails or conditions are not met, this try block will complete,
            # and the function will proceed to other parsing attempts or the final fallback.
        except (ValueError, IndexError): # Catches errors from strptime or list indexing if parts are not as expected
            pass # If this custom parsing fails, move to next methods

        # Handle relative dates like "1 day ago", "1 hr ago"
        if "ago" in date_str or "hr" in date_str or "min" in date_str:  # simple handling for recent posts
            # For "X days/hours/mins ago", approximate to today's date.
            # More precise parsing would require libraries like `dateparser`
            # or more complex logic to subtract the duration.
            # For simplicity in this script, we'll use today's date.
            return datetime.now().strftime("%Y-%m-%d")
        try:
            # Try parsing "Mar 23" (assuming current year if year is missing)
            dt_object = datetime.strptime(date_str + f", {datetime.now().year}", "%b %d, %Y")
            return dt_object.strftime("%Y-%m-%d")
        except ValueError:
            # Try parsing "YYYY-MM-DD" style dates if they already exist
            try:
                dt_object = datetime.strptime(date_str, "%Y-%m-%d")
                return dt_object.strftime("%Y-%m-%d")  # Already in correct format
            except ValueError:
                print(f"Warning: Could not parse date string: '{date_str}'. Using original string.")
                return date_str  # Fallback to original string if all parsing fails


def generate_html_file(author_name: str) -> None:
    """
    Generates a HTML file for the given author.
    """
    if not os.path.exists(BASE_HTML_DIR):
        os.makedirs(BASE_HTML_DIR)

    # Read JSON data
    json_path = os.path.join(JSON_DATA_DIR, f'{author_name}.json')
    with open(json_path, 'r', encoding='utf-8') as file:
        essays_data = json.load(file)

    # Convert JSON data to a JSON string for embedding
    embedded_json_data = json.dumps(essays_data, ensure_ascii=False, indent=4)

    with open(HTML_TEMPLATE, 'r', encoding='utf-8') as file:
        html_template = file.read()

    # Insert the JSON string into the script tag in the HTML template
    html_with_data = html_template.replace('<!-- AUTHOR_NAME -->', author_name).replace(
        '<script type="application/json" id="essaysData"></script>',
        f'<script type="application/json" id="essaysData">{embedded_json_data}</script>'
    )
    html_with_author = html_with_data.replace('author_name', author_name)

    # Write the modified HTML to a new file
    html_output_path = os.path.join(BASE_HTML_DIR, f'{author_name}.html')
    with open(html_output_path, 'w', encoding='utf-8') as file:
        file.write(html_with_author)


class BaseSubstackScraper(ABC):
    def __init__(self, base_substack_url: str, md_save_dir: str, html_save_dir: str):
        if not base_substack_url.endswith("/"):
            base_substack_url += "/"
        self.base_substack_url: str = base_substack_url

        self.writer_name: str = extract_main_part(base_substack_url)
        md_save_dir: str = f"{md_save_dir}/{self.writer_name}"

        self.md_save_dir: str = md_save_dir
        self.html_save_dir: str = f"{html_save_dir}/{self.writer_name}"

        if not os.path.exists(md_save_dir):
            os.makedirs(md_save_dir)
            print(f"Created md directory {md_save_dir}")
        if not os.path.exists(self.html_save_dir):
            os.makedirs(self.html_save_dir)
            print(f"Created html directory {self.html_save_dir}")

        self.keywords: List[str] = ["about", "archive", "podcast"]
        self.feed_item_contents: dict[str, str] = {}
        self.post_urls: List[str] = self._get_all_post_urls_and_feed_content()

    def _get_all_post_urls_and_feed_content(self) -> List[str]:
        """
        Fetches URLs primarily from sitemap.xml.
        Also fetches content from feed.xml to enable pre-emptive premium checks.
        Falls back to feed.xml for URLs if sitemap.xml fails.
        """
        sitemap_urls = self._fetch_urls_from_sitemap()
        feed_urls_and_content = self._fetch_urls_and_content_from_feed()

        # Populate self.feed_item_contents from feed_urls_and_content
        for url, content in feed_urls_and_content.items():
            self.feed_item_contents[url] = content

        if sitemap_urls:
            # Combine URLs, prioritizing sitemap for completeness, but ensuring all feed URLs are included
            # (though feed URLs should mostly be a subset of sitemap URLs)
            combined_urls = list(sitemap_urls)
            for url in feed_urls_and_content.keys():
                if url not in combined_urls:
                    combined_urls.append(url)
            # Filter all collected URLs
            return self.filter_urls(combined_urls, self.keywords)
        elif feed_urls_and_content:
            # Fallback to URLs from feed if sitemap failed
            print("Sitemap.xml failed or was empty, using URLs from feed.xml.")
            return self.filter_urls(list(feed_urls_and_content.keys()), self.keywords)
        else:
            # No URLs from either source
            print("Could not retrieve URLs from sitemap.xml or feed.xml.")
            return []

    def _fetch_urls_from_sitemap(self) -> List[str]:
        """
        Fetches URLs from sitemap.xml.
        """
        sitemap_url = f"{self.base_substack_url}sitemap.xml"
        try:
            response = requests.get(sitemap_url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            urls = [element.text for element in root.iter('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]
            return urls
        except requests.exceptions.RequestException as e:
            print(f'Error fetching sitemap at {sitemap_url}: {e}')
            return []
        except ET.ParseError as e:
            print(f'Error parsing sitemap XML from {sitemap_url}: {e}')
            return []


    def _fetch_urls_and_content_from_feed(self) -> dict[str, str]:
        """
        Fetches URLs and their <content:encoded> from feed.xml.
        Returns a dictionary mapping URL to its content string.
        """
        feed_url = f"{self.base_substack_url}feed" # .xml is often optional, /feed is common
        urls_and_content = {}
        try:
            response = requests.get(feed_url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            # Namespace for content:encoded might vary, common is 'http://purl.org/rss/1.0/modules/content/'
            # ET.fromstring doesn't handle prefixed tags like content:encoded directly in find unless namespace is registered
            # A simpler way for known structures is to iterate and check tag names.

            namespaces = {'content': 'http://purl.org/rss/1.0/modules/content/'} # Common namespace

            for item in root.findall('.//item'):
                link_element = item.find('link')
                content_element = item.find('content:encoded', namespaces)

                if link_element is not None and link_element.text:
                    url = link_element.text
                    content_html = content_element.text if content_element is not None else ""
                    urls_and_content[url] = content_html

            if not urls_and_content:
                 print(f"No items found in feed.xml or feed content missing at {feed_url}")

        except requests.exceptions.RequestException as e:
            print(f'Error fetching feed at {feed_url}: {e}')
        except ET.ParseError as e:
            print(f'Error parsing feed XML from {feed_url}: {e}')

        return urls_and_content

    @staticmethod
    def filter_urls(urls: List[str], keywords: List[str]) -> List[str]:
        """
        This method filters out URLs that contain certain keywords
        """
        return [url for url in urls if all(keyword not in url for keyword in keywords)]

    @staticmethod
    def html_to_md(html_content: str) -> str:
        """
        This method converts HTML to Markdown
        """
        if not isinstance(html_content, str):
            raise ValueError("html_content must be a string")
        h = html2text.HTML2Text()
        h.ignore_links = False
        h.body_width = 0
        return h.handle(html_content)

    @staticmethod
    def save_to_file(filepath: str, content: str) -> None:
        """
        This method saves content to a file. Can be used to save HTML or Markdown
        """
        if not isinstance(filepath, str):
            raise ValueError("filepath must be a string")

        if not isinstance(content, str):
            raise ValueError("content must be a string")

        if os.path.exists(filepath):
            print(f"File already exists: {filepath}")
            return

        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(content)

    @staticmethod
    def md_to_html(md_content: str) -> str:
        """
        This method converts Markdown to HTML
        """
        return markdown.markdown(md_content, extensions=['extra'])

    def save_to_html_file(self, filepath: str, content: str) -> None:
        """
        This method saves HTML content to a file with a link to an external CSS file.
        """
        if not isinstance(filepath, str):
            raise ValueError("filepath must be a string")

        if not isinstance(content, str):
            raise ValueError("content must be a string")

        # Calculate the relative path from the HTML file to the CSS file
        html_dir = os.path.dirname(filepath)
        css_path = os.path.relpath("./assets/css/essay-styles.css", html_dir)
        css_path = css_path.replace("\\", "/")  # Ensure forward slashes for web paths

        html_content = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Markdown Content</title>
                <link rel="stylesheet" href="{css_path}">
            </head>
            <body>
                <main class="markdown-content">
                {content}
                </main>
            </body>
            </html>
        """

        with open(filepath, 'w', encoding='utf-8') as file:
            file.write(html_content)

    @staticmethod
    def get_filename_from_url(url: str, filetype: str = ".md") -> str:
        """
        Gets the filename from the URL (the ending)
        """
        if not isinstance(url, str):
            raise ValueError("url must be a string")

        if not isinstance(filetype, str):
            raise ValueError("filetype must be a string")

        if not filetype.startswith("."):
            filetype = f".{filetype}"

        return url.split("/")[-1] + filetype

    @staticmethod
    def combine_metadata_and_content(title: str, subtitle: str, date: str, like_count: str, content) -> str:
        """
        Combines the title, subtitle, and content into a single string with Markdown format
        """
        if not isinstance(title, str):
            raise ValueError("title must be a string")

        if not isinstance(content, str):
            raise ValueError("content must be a string")

        metadata = f"# {title}\n\n"
        if subtitle:
            metadata += f"## {subtitle}\n\n"
        metadata += f"**{date}**\n\n"
        metadata += f"**Likes:** {like_count}\n\n"

        return metadata + content

    def extract_post_data(self, soup: BeautifulSoup) -> Tuple[str, str, str, str, str]:
        """
        Converts substack post soup to markdown, returns metadata and content
        """
        title_element = soup.select_one("h1.post-title, h2")
        title = title_element.text.strip() if title_element else "Title not found"

        subtitle_element = soup.select_one("h3.subtitle")
        subtitle = subtitle_element.text.strip() if subtitle_element else ""

        date_element = soup.find(
            "div",
            class_="pencraft pc-reset color-pub-secondary-text-hGQ02T line-height-20-t4M0El font-meta-MWBumP size-11-NuY2Zx weight-medium-fw81nC transform-uppercase-yKDgcq reset-IxiVJZ meta-EgzBVA"
        )
        raw_date_full = date_element.text.strip() if date_element else "Date not found"

        # Attempt to isolate the date part if it's appended with " - "
        if " - " in raw_date_full:
            parts = raw_date_full.split(" - ")
            # Assume the date is the last part. This is a heuristic.
            # Further validation could be added here if needed (e.g., regex for date-like pattern)
            raw_date_isolated = parts[-1]
        else:
            raw_date_isolated = raw_date_full

        date = format_substack_date(raw_date_isolated)

        like_count_element = soup.select_one("a.post-ufi-button .label")
        like_count = (
            like_count_element.text.strip()
            if like_count_element and like_count_element.text.strip().isdigit()
            else "0"
        )

        content_element = soup.select_one("div.available-content")
        content_html = str(content_element) if content_element else "<p>Content not found</p>"
        # Using "<p>Content not found</p>" so html_to_md still gets valid HTML,
        # which will translate to "Content not found" in markdown.

        md = self.html_to_md(content_html)
        md_content = self.combine_metadata_and_content(title, subtitle, date, like_count, md)
        return title, subtitle, like_count, date, md_content

    @abstractmethod
    def get_url_soup(self, url: str) -> str:
        raise NotImplementedError

    def save_essays_data_to_json(self, essays_data: list) -> None:
        """
        Saves essays data to a JSON file for a specific author.
        """
        data_dir = os.path.join(JSON_DATA_DIR)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        json_path = os.path.join(data_dir, f'{self.writer_name}.json')
        if os.path.exists(json_path):
            with open(json_path, 'r', encoding='utf-8') as file:
                existing_data = json.load(file)
            essays_data = existing_data + [data for data in essays_data if data not in existing_data]
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(essays_data, f, ensure_ascii=False, indent=4)

    def create_epub_from_author_markdown(self, author_name: str, base_md_dir: str, base_html_dir: str,
                                         json_data_dir: str) -> None:
        """
        Creates an EPUB file from the scraped markdown posts for a given author.

        Args:
            author_name: The name of the Substack author.
            base_md_dir: The base directory where markdown files are stored.
            base_html_dir: The base directory where HTML files are stored (unused in current EPUB logic but passed for consistency).
            json_data_dir: The directory where JSON metadata files are stored.
        """
        print(f"Starting EPUB generation for {author_name}...")

        json_path = os.path.join(json_data_dir, f'{author_name}.json')
        if not os.path.exists(json_path):
            print(f"Error: JSON data file not found for {author_name} at {json_path}. Cannot generate EPUB.")
            return

        with open(json_path, 'r', encoding='utf-8') as f:
            posts_metadata = json.load(f)

        if not posts_metadata:
            print(f"No posts found in JSON data for {author_name}. EPUB will be empty.")
            return

        # Sort posts by date. Handles "Unknown Date" by placing them at the end or beginning based on preference.
        # Here, 'Unknown Date' will cause an error if not handled before sorting.
        # We will filter out entries with "Unknown Date" or handle them by assigning a placeholder date.
        valid_posts = []
        for post in posts_metadata:
            if post.get('date') and post['date'] != "Unknown Date" and post['date'] != "Date not found":
                try:
                    # Ensure date is in a comparable format if it's already YYYY-MM-DD
                    datetime.strptime(post['date'], "%Y-%m-%d")
                    valid_posts.append(post)
                except ValueError:
                    print(
                        f"Skipping post with invalid date format: {post.get('title', 'Unknown Title')} - {post.get('date')}")
            else:
                print(f"Skipping post with missing or unknown date: {post.get('title', 'Unknown Title')}")

        # Sort posts by date, then by title as a secondary key if dates are the same
        # The primary sort key is 'date'.
        # If 'title' is missing, use a placeholder string.
        sorted_posts = sorted(valid_posts, key=lambda x: (x['date'], x.get('title', '')))

        book = epub.EpubBook()
        book.set_identifier(f"urn:uuid:{author_name}-{datetime.now().timestamp()}")
        book.set_title(f"{author_name.replace('_', ' ').title()} Substack Archive")
        book.set_language("en")
        book.add_author(author_name.replace('_', ' ').title())

        # Define TOC and chapters list
        chapters = []
        toc = []

        # Create a directory for EPUBs if it doesn't exist
        if not os.path.exists(BASE_EPUB_DIR):
            os.makedirs(BASE_EPUB_DIR)

        author_epub_dir = os.path.join(BASE_EPUB_DIR, author_name)
        if not os.path.exists(author_epub_dir):
            os.makedirs(author_epub_dir)

        epub_filename = os.path.join(author_epub_dir, f"{author_name}_substack_archive.epub")

        # Default CSS for styling the EPUB content
        default_css = epub.EpubItem(
            uid="style_default",
            file_name="style/default.css",
            media_type="text/css",
            content="""
                body { font-family: serif; line-height: 1.6; }
                h1, h2, h3, h4, h5, h6 { font-family: sans-serif; }
                img { max-width: 100%; height: auto; }
                pre { white-space: pre-wrap; word-wrap: break-word; background-color: #f4f4f4; padding: 10px; border-radius: 4px; }
                code { font-family: monospace; }
            """
        )
        book.add_item(default_css)

        for i, post_meta in enumerate(sorted_posts):
            md_filepath = post_meta.get("file_link")
            if not md_filepath or not os.path.exists(md_filepath):
                print(
                    f"Warning: Markdown file not found for post: {post_meta.get('title', 'Unknown Title')}. Skipping.")
                continue

            with open(md_filepath, 'r', encoding='utf-8') as md_file:
                markdown_content = md_file.read()

            # Convert Markdown to HTML. The `markdown` library is already a dependency.
            # We strip the existing metadata from the markdown content before converting to HTML for the EPUB body
            # as EPUB will have its own metadata.
            # A simple way to strip metadata is to find the first occurrence of "\n\n**Likes:**"
            # and take content after that, or more robustly, find the end of the metadata block.
            # For now, let's assume metadata is at the start and ends before the main content.
            # A common pattern is that content starts after the second `\n\n` if there's a subtitle, or first if no subtitle.

            # Revised logic to remove metadata from the top of the .md file
            # Looks for the line containing "**Likes:**"
            metadata_marker = "**Likes:**"
            # Find the start of the metadata_marker, but be flexible with preceding newlines.
            # We search for the marker that could be at the start of a line or after some text (like a date).
            # A simple find should be okay if the marker is distinct enough.
            likes_line_start_index = markdown_content.find(metadata_marker)

            if likes_line_start_index != -1:
                # Find the end of the line containing "**Likes:**"
                # This is the position of the first newline character after the marker's occurrence.
                end_of_likes_line = markdown_content.find("\n", likes_line_start_index + len(metadata_marker))
                if end_of_likes_line != -1:
                    # Content starts after this newline.
                    content_after_likes_line = markdown_content[end_of_likes_line + 1:]  # +1 to move past the \n
                    # Strip leading whitespace (including newlines) from the extracted content
                    actual_content_markdown = content_after_likes_line.lstrip()
                else:
                    # This case means "**Likes:**" was found, but it's the very last thing in the file (no newline after it).
                    actual_content_markdown = ""
            else:
                # If "**Likes:**" is not found, assume no metadata or a different format;
                # use all content. This part of the logic remains the same.
                # This could happen if posts have no likes or the format changes.
                actual_content_markdown = markdown_content

            html_content = markdown.markdown(actual_content_markdown, extensions=['extra', 'meta'])

            chapter_title = post_meta.get("title", f"Chapter {i + 1}")
            # Sanitize filename for EPUB internal use
            chapter_filename_sanitized = "".join(c if c.isalnum() else "_" for c in chapter_title[:50])
            chapter_filename = f"chap_{i + 1}_{chapter_filename_sanitized}.xhtml"

            # Create chapter
            # Need to ensure html_content is bytes, not str.
            # Also, title should be a string.
            epub_chapter = epub.EpubHtml(title=str(chapter_title), file_name=chapter_filename, lang="en")

            # Basic HTML structure for the chapter content
            full_html_content = f"""<!DOCTYPE html>
            <html xmlns="http://www.w3.org/1999/xhtml" lang="en">
            <head>
                <meta charset="utf-8" />
                <title>{str(chapter_title)}</title>
                <link rel="stylesheet" type="text/css" href="style/default.css" />
            </head>
            <body>
                <h1>{str(chapter_title)}</h1>
                {html_content}
            </body>
            </html>"""

            epub_chapter.content = full_html_content.encode('utf-8')  # Ensure content is bytes
            epub_chapter.add_item(default_css)  # Link CSS to this chapter
            book.add_item(epub_chapter)
            chapters.append(epub_chapter)

            # Add date to TOC title
            post_date = post_meta.get("date", "Unknown Date")
            if post_date == "Unknown Date" or post_date == "Date not found":
                toc_title = str(chapter_title)
            else:
                # Ensure date is formatted as YYYY-MM-DD before appending
                try:
                    # Assuming post_date is already in "YYYY-MM-DD" after format_substack_date
                    # If not, re-format or ensure it is.
                    datetime.strptime(post_date, "%Y-%m-%d") # Validate format
                    toc_title = f"{str(chapter_title)} ({post_date})"
                except ValueError:
                    # If date is not in YYYY-MM-DD, use it as is or log warning
                    toc_title = f"{str(chapter_title)} ({post_date})" # Fallback or handle error

            toc.append(epub.Link(chapter_filename, toc_title, f"chap_{i + 1}"))

        # Define Table of Contents
        book.toc = tuple(toc)

        # Add default NCX and Nav file
        book.add_item(epub.EpubNcx())
        book.add_item(epub.EpubNav())

        # Set the spine (order of chapters)
        # The first item in spine is often the cover or title page, then Nav, then chapters.
        # For simplicity, we'll just list the chapters.
        # To include the Nav in the spine (as some readers prefer):
        # book.spine = ['nav'] + chapters
        # Or, if you have a cover:
        # book.spine = ['cover', 'nav'] + chapters
        # For now, just chapters:
        book.spine = ['nav'] + chapters  # Nav should usually come first for navigation structure

        # Get the number of posts successfully added to the EPUB
        post_count = len(chapters)

        try:
            epub.write_epub(epub_filename, book, {})
            print(f"Successfully generated EPUB: {epub_filename} with {post_count} posts.")
        except Exception as e:
            print(f"Error writing EPUB file for {author_name}: {e}")

    def scrape_posts(self, num_posts_to_scrape: int = 0) -> None:
        """
        Iterates over all posts and saves them as markdown and html files
        """
        essays_data = []
        count = 0
        total = num_posts_to_scrape if num_posts_to_scrape != 0 else len(self.post_urls)
        # Initialize tqdm with the correct total number of items to process.
        # If num_posts_to_scrape is set, it limits how many new posts we attempt to scrape.
        # If 0, we iterate through all fetched post_urls.

        # We use a progress bar for the number of URLs we intend to process.
        # If a URL is skipped (e.g. already exists, or fails to download/parse),
        # tqdm will still advance for that iteration.

        processed_urls_count = 0 # To keep track of successfully processed posts for the num_posts_to_scrape limit

        for url in tqdm(self.post_urls, total=total, desc=f"Scraping {self.writer_name}"):
            if num_posts_to_scrape != 0 and processed_urls_count >= num_posts_to_scrape:
                print(f"Reached scrape limit of {num_posts_to_scrape} posts.")
                break # Exit the loop if the desired number of posts has been scraped

            try:
                md_filename = self.get_filename_from_url(url, filetype=".md")
                html_filename = self.get_filename_from_url(url, filetype=".html")
                md_filepath = os.path.join(self.md_save_dir, md_filename)
                html_filepath = os.path.join(self.html_save_dir, html_filename)

                if not os.path.exists(md_filepath):
                    should_skip_fetching = False
                    if isinstance(self, SubstackScraper):
                        if self.is_article_premium_from_feed(url):
                            print(f"Skipping premium article (feed check): {url}")
                            should_skip_fetching = True

                    if should_skip_fetching:
                        continue # Skip to the next URL

                    # If we are here, the article is either:
                    # 1. Not premium (for SubstackScraper, based on feed check)
                    # 2. Being processed by PremiumSubstackScraper (which doesn't use the feed check to skip here)
                    # In both cases, if the file doesn't exist, we sleep before fetching.
                    print(f"Pausing for {DELAY_LENGTH} seconds before fetching article: {url}")
                    sleep(DELAY_LENGTH)

                    soup = self.get_url_soup(url)
                    if soup is None:
                        # If soup is None, get_url_soup might have printed a message (e.g. premium detected on page).
                        # We just continue to the next URL. tqdm will advance.
                        continue

                    title, subtitle, like_count, date, md = self.extract_post_data(soup)
                    self.save_to_file(md_filepath, md)

                    # Convert markdown to HTML and save
                    html_content = self.md_to_html(md)
                    self.save_to_html_file(html_filepath, html_content)

                    essays_data.append({
                        "title": title,
                        "subtitle": subtitle,
                        "like_count": like_count,
                        "date": date,
                        "file_link": md_filepath,
                        "html_link": html_filepath
                    })
                    count += 1 # Increment count only for successfully processed new posts
                else:
                    print(f"File already exists: {md_filepath}")
            except Exception as e:
                print(f"Error scraping post: {e} for URL: {url}") # Added URL to error message

            # Check if the desired number of posts has been scraped
            if num_posts_to_scrape != 0 and count >= num_posts_to_scrape: # Changed to >=
                print(f"Reached scrape limit of {num_posts_to_scrape} successfully processed new posts.")
                break

        self.save_essays_data_to_json(essays_data=essays_data)
        generate_html_file(author_name=self.writer_name)

        # Call EPUB generation
        # Need to pass the base directories, not the author-specific ones
        # self.md_save_dir is like "substack_md_files/author_name"
        # We need "substack_md_files"
        parent_md_dir = os.path.dirname(self.md_save_dir)
        parent_html_dir = os.path.dirname(self.html_save_dir)
        self.create_epub_from_author_markdown(
            author_name=self.writer_name,
            base_md_dir=parent_md_dir,
            base_html_dir=parent_html_dir,
            json_data_dir=JSON_DATA_DIR
        )


class SubstackScraper(BaseSubstackScraper):
    def __init__(self, base_substack_url: str, md_save_dir: str, html_save_dir: str):
        super().__init__(base_substack_url, md_save_dir, html_save_dir)

    def is_article_premium_from_feed(self, url: str) -> bool:
        """
        Checks if an article is premium based on its content in the feed.
        Prioritizes specific patterns and falls back to more general text checks.
        Returns True if premium, False otherwise.
        """
        if hasattr(self, 'feed_item_contents') and url in self.feed_item_contents:
            feed_html_snippet = self.feed_item_contents[url]
            if feed_html_snippet:  # Ensure there's content to parse
                feed_soup = BeautifulSoup(feed_html_snippet, "html.parser")
                # Check for common Substack paywall patterns in RSS feed snippets
                # 1. Explicit "paid subscribers" text
                paid_subscriber_texts = [
                    "this post is for paid subscribers",
                    "to read the full post, subscribe",
                    "this is a preview of a paid post",
                    "upgrade to paid"
                ]
                if any(text.lower() in feed_soup.get_text().lower() for text in paid_subscriber_texts):
                    return True

                # 2. Presence of a prominent "subscribe" button that's likely a paywall CTA
                subscribe_button = feed_soup.find("a", class_="button", href=lambda x: x and "subscribe?" in x)
                if subscribe_button:
                    description_element = feed_soup.find("meta", property="og:description")
                    description_text = description_element['content'].lower() if description_element and description_element.get('content') else ""
                    if "subscribe" in subscribe_button.text.lower() and ("only for subscribers" in description_text or "paid post" in description_text):
                        return True
        return False

    def get_url_soup(self, url: str) -> Optional[BeautifulSoup]:
        """
        Gets soup from URL using requests.
        Returns BeautifulSoup object or None if fetching/parsing fails or content is missing.
        """
        # The pre-emptive feed check is now done in scrape_posts before calling this method
        # for SubstackScraper instances.
        # However, we keep a simplified version of the feed check here as a fallback
        # or if get_url_soup is called directly.
        if self.is_article_premium_from_feed(url):
            print(f"Skipping premium article (detected from feed preview by get_url_soup): {url}")
            return None

        try:
            # If not skipped by feed check, proceed with existing logic
            page = requests.get(url, headers=None, timeout=10) # Added timeout
            page.raise_for_status()  # Raise HTTPError for bad responses (4XX or 5XX)
            soup = BeautifulSoup(page.content, "html.parser")

            # Check for paywall on the actual page (this is the original check)
            if soup.find("h2", class_="paywall-title"):
                print(f"Skipping premium article (detected on page): {url}")
                return None

            # Preemptive check for essential content elements
            # If neither title-like elements nor the main content div is found,
            # the page structure might be unexpected or an error page.
            has_title_elements = soup.select_one("h1.post-title, h2")
            has_content_div = soup.select_one("div.available-content")

            if not has_title_elements and not has_content_div:
                print(f"Warning: Essential content (title or body) missing from {url}. Skipping.")
                return None

            return soup
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error fetching page {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {url}: {e}")
            return None
        except Exception as e:
            # Catch any other unexpected errors during parsing (e.g., BeautifulSoup issues)
            print(f"An unexpected error occurred while processing {url}: {e}")
            return None


class PremiumSubstackScraper(BaseSubstackScraper):
    def __init__(
            self,
            base_substack_url: str,
            md_save_dir: str,
            html_save_dir: str,
            headless: bool = False,
            edge_path: str = '',
            edge_driver_path: str = '',
            user_agent: str = ''
    ) -> None:
        super().__init__(base_substack_url, md_save_dir, html_save_dir)

        options = EdgeOptions()
        if headless:
            options.add_argument("--headless")
        if edge_path:
            options.binary_location = edge_path
        if user_agent:
            options.add_argument(f'user-agent={user_agent}')  # Pass this if running headless and blocked by captcha

        if edge_driver_path:
            service = Service(executable_path=edge_driver_path)
        else:
            service = Service(EdgeChromiumDriverManager().install())

        self.driver = webdriver.Edge(service=service, options=options)
        self.login()

    def login(self) -> None:
        """
        This method logs into Substack using Selenium
        """
        self.driver.get("https://substack.com/sign-in")
        sleep(3)

        signin_with_password = self.driver.find_element(
            By.XPATH, "//a[@class='login-option substack-login__login-option']"
        )
        signin_with_password.click()
        sleep(3)

        # Email and password
        email = self.driver.find_element(By.NAME, "email")
        password = self.driver.find_element(By.NAME, "password")
        email.send_keys(EMAIL)
        password.send_keys(PASSWORD)

        # Find the submit button and click it.
        submit = self.driver.find_element(By.XPATH, "//*[@id=\"substack-login\"]/div[2]/div[2]/form/button")
        submit.click()
        sleep(30)  # Wait for the page to load

        if self.is_login_failed():
            raise Exception(
                "Warning: Login unsuccessful. Please check your email and password, or your account status.\n"
                "Use the non-premium scraper for the non-paid posts. \n"
                "If running headless, run non-headlessly to see if blocked by Captcha."
            )

    def is_login_failed(self) -> bool:
        """
        Check for the presence of the 'error-container' to indicate a failed login attempt.
        """
        error_container = self.driver.find_elements(By.ID, 'error-container')
        return len(error_container) > 0 and error_container[0].is_displayed()

    def get_url_soup(self, url: str) -> BeautifulSoup:
        """
        Gets soup from URL using logged in selenium driver
        """
        try:
            self.driver.get(url)
            return BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            raise ValueError(f"Error fetching page: {e}") from e


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape a Substack site.")
    parser.add_argument(
        "-u", "--url", type=str, help="The base URL of the Substack site to scrape."
    )
    parser.add_argument(
        "-d", "--directory", type=str, help="The directory to save scraped posts."
    )
    parser.add_argument(
        "-n",
        "--number",
        type=int,
        default=0,
        help="The number of posts to scrape. If 0 or not provided, all posts will be scraped.",
    )
    parser.add_argument(
        "-p",
        "--premium",
        action="store_true",
        help="Include -p in command to use the Premium Substack Scraper with selenium.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Include -h in command to run browser in headless mode when using the Premium Substack "
             "Scraper.",
    )
    parser.add_argument(
        "--edge-path",
        type=str,
        default="",
        help='Optional: The path to the Edge browser executable (i.e. "path_to_msedge.exe").',
    )
    parser.add_argument(
        "--edge-driver-path",
        type=str,
        default="",
        help='Optional: The path to the Edge WebDriver executable (i.e. "path_to_msedgedriver.exe").',
    )
    parser.add_argument(
        "--user-agent",
        type=str,
        default="",
        help="Optional: Specify a custom user agent for selenium browser automation. Useful for "
             "passing captcha in headless mode",
    )
    parser.add_argument(
        "--html-directory",
        type=str,
        help="The directory to save scraped posts as HTML files.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.directory is None:
        args.directory = BASE_MD_DIR

    if args.html_directory is None:
        args.html_directory = BASE_HTML_DIR

    if args.url:
        urls_to_scrape = [args.url]
    else:
        urls_to_scrape = SUBSTACK_URLS

    for url in urls_to_scrape:
        print(f"Scraping {url}...")
        if args.premium or USE_PREMIUM:
            scraper = PremiumSubstackScraper(
                base_substack_url=url,
                headless=args.headless,
                md_save_dir=args.directory,
                html_save_dir=args.html_directory,
                edge_path=args.edge_path,
                edge_driver_path=args.edge_driver_path,
                user_agent=args.user_agent
            )
        else:
            scraper = SubstackScraper(
                base_substack_url=url,
                md_save_dir=args.directory,
                html_save_dir=args.html_directory
            )

        num_posts = args.number if args.url else NUM_POSTS_TO_SCRAPE
        scraper.scrape_posts(num_posts_to_scrape=num_posts)


if __name__ == "__main__":
    main()
