#!/usr/bin/env python3
"""
Categorize pages using LLM based on title + description.
Replaces the auto-extracted topics with meaningful categories.
"""

import json
import os
import db
import httpx

API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
if not API_KEY:
    # Try to read from common locations
    for path in ["~/.anthropic/api_key", "~/.config/anthropic/api_key"]:
        expanded = os.path.expanduser(path)
        if os.path.exists(expanded):
            API_KEY = open(expanded).read().strip()
            break

# Broad educational categories
CATEGORIES = """
- Physics / Light & Optics
- Physics / Forces & Motion  
- Physics / Energy
- Chemistry
- Biology / Life Science
- Earth Science / Weather
- Earth Science / Geology
- Space / Astronomy
- Math
- Engineering / Technology
- Environmental Science
- Health / Human Body
- History of Science
- Science Methods / Experiments
- General Science Education
"""

def categorize_batch(pages: list[dict]) -> dict[int, list[str]]:
    """Categorize multiple pages in one API call."""
    
    pages_text = ""
    for i, p in enumerate(pages):
        pages_text += f"""
Page {p['id']}:
  Title: {p['title'] or 'No title'}
  Description: {(p['description'] or 'No description')[:300]}
  Domain: {p['domain']}
"""

    prompt = f"""Categorize these educational pages. For each page, assign 1-3 relevant topic categories.

{pages_text}

Choose from these categories (or suggest a better one if clearly needed):
{CATEGORIES}

Reply in this exact format:
PAGE_ID: Category 1, Category 2
PAGE_ID: Category 1

Example:
42: Physics / Light & Optics, Science Methods / Experiments
43: Earth Science / Weather"""

    response = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}]
        },
        timeout=60
    )
    
    if response.status_code != 200:
        print(f"API error: {response.status_code} {response.text}")
        return {}
    
    result = response.json()
    text = result["content"][0]["text"]
    
    # Parse response
    categories = {}
    for line in text.strip().split('\n'):
        if ':' in line:
            parts = line.split(':', 1)
            try:
                page_id = int(parts[0].strip())
                cats = [c.strip() for c in parts[1].split(',')]
                categories[page_id] = cats
            except (ValueError, IndexError):
                continue
    
    return categories


def main():
    if not API_KEY:
        print("Error: No ANTHROPIC_API_KEY found")
        print("Set it in environment or ~/.anthropic/api_key")
        return
    
    print("=" * 60)
    print("PAGE CATEGORIZER")
    print("=" * 60)
    
    conn = db.get_conn()
    pages = conn.execute("""
        SELECT id, url, title, description, domain 
        FROM pages 
        ORDER BY id
    """).fetchall()
    conn.close()
    
    print(f"Found {len(pages)} pages to categorize\n")
    
    # Clear existing topics
    conn = db.get_conn()
    conn.execute("DELETE FROM page_topics")
    conn.execute("DELETE FROM topics")
    conn.commit()
    conn.close()
    
    # Process in batches of 10
    batch_size = 10
    all_categories = {}
    
    for i in range(0, len(pages), batch_size):
        batch = pages[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(pages) + batch_size - 1)//batch_size}...")
        
        batch_dicts = [dict(p) for p in batch]
        cats = categorize_batch(batch_dicts)
        all_categories.update(cats)
        
        for page_id, page_cats in cats.items():
            print(f"  Page {page_id}: {', '.join(page_cats)}")
    
    # Save to database
    print("\nSaving to database...")
    for page_id, cats in all_categories.items():
        for cat in cats:
            topic_id = db.get_or_create_topic(cat)
            db.link_page_topic(page_id, topic_id)
    
    # Show results
    print("\n" + "=" * 60)
    print("FINAL CATEGORIES")
    print("=" * 60)
    
    for t in db.get_topics():
        print(f"  {t['name']}: {t['page_count']} pages")
    
    print(f"\nDone! {len(db.get_topics())} categories from {len(pages)} pages.")


if __name__ == "__main__":
    main()
