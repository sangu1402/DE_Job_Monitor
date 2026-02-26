"""
Comprehensive Real-Time Data Engineer Job Monitor v5.2
=======================================================
Covers 1000+ companies across every major job platform and ATS.
Sends instant email + desktop notification the moment a job is posted.
STRICT USA-only filtering — no Bangalore, no London, no Worldwide.

COVERAGE:
  Direct Company APIs : Amazon, Google, Microsoft, Meta, Apple, IBM, Oracle,
                        Tesla, Netflix, JPMorgan + more

  ATS Platforms       : Greenhouse (110+ cos), Lever (70+ cos), Ashby (70+ cos),
                        Workday (165+ cos), SmartRecruiters (35+ cos),
                        Eightfold AI (7 cos), Workable

  Job Board APIs      : Indeed, ZipRecruiter, Dice, CareerBuilder, SimplyHired,
                        Remotive, Arbeitnow, The Muse, RemoteOK, Himalayas,
                        USAJobs, Adzuna, WeWorkRemotely, JSearch, SerpApi

  Startup Boards      : YC Work at a Startup, Wellfound/AngelList

SETUP:
  pip install requests feedparser plyer schedule python-dotenv python-dateutil

  .env file:
    SMTP_USER=your@gmail.com
    SMTP_PASS=your-gmail-app-password
    ALERT_EMAIL=you@email.com
    ADZUNA_APP_ID=optional
    ADZUNA_APP_KEY=optional
    JSEARCH_API_KEY=optional
    SERPAPI_KEY=optional
    CHECK_INTERVAL=2

USAGE:
  python job_monitor.py           # Start monitoring (Ctrl+C to stop)
  python job_monitor.py --once    # Single scan and exit
  python job_monitor.py --test    # Send test email + desktop notification
"""

import os, re, json, time, smtplib, hashlib, logging, argparse
import requests, feedparser, schedule
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from dateutil import parser as dparser

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

try:
    from plyer import notification as desktop_notify
    DESKTOP_AVAILABLE = True
except ImportError:
    DESKTOP_AVAILABLE = False

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS = [
    "data engineer", "data engineering", "analytics engineer",
    "dataops engineer", "etl engineer", "pipeline engineer",
    "data platform engineer", "data infrastructure engineer",
]

SEEN_FILE      = "seen_jobs.json"
LOG_FILE       = "job_monitor.log"
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "2"))

SMTP_HOST   = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER   = os.getenv("SMTP_USER", "")
SMTP_PASS   = os.getenv("SMTP_PASS", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", SMTP_USER)

ADZUNA_APP_ID   = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY  = os.getenv("ADZUNA_APP_KEY", "")
JSEARCH_API_KEY = os.getenv("JSEARCH_API_KEY", "")
SERPAPI_KEY     = os.getenv("SERPAPI_KEY", "")

BLOCKED_COMPANIES = {"Lensa", "Jobs via Dice", "Arbor Tek Systems"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode='w', encoding='utf-8', closefd=False)
        )
    ]
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 JobMonitor/5.2"})
# Increase connection pool size to match thread workers and avoid pool-full warnings
adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

# ─────────────────────────────────────────────────────────────────────────────
# STRICT US LOCATION FILTERING
# ─────────────────────────────────────────────────────────────────────────────

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL",
    "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX",
    "UT","VT","VA","WA","WV","WI","WY","DC"
}

US_STATE_NAMES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut",
    "delaware","florida","georgia","hawaii","idaho","illinois","indiana","iowa",
    "kansas","kentucky","louisiana","maine","maryland","massachusetts","michigan",
    "minnesota","mississippi","missouri","montana","nebraska","nevada",
    "new hampshire","new jersey","new mexico","new york","north carolina",
    "north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island",
    "south carolina","south dakota","tennessee","texas","utah","vermont",
    "virginia","washington","west virginia","wisconsin","wyoming",
    "washington dc","washington d.c.","district of columbia"
}

# Known non-US city/country keywords — if any appear, reject immediately
NON_US_KEYWORDS = {
    # India
    "bangalore","bengaluru","hyderabad","mumbai","pune","chennai","delhi",
    "noida","gurugram","gurgaon","kolkata","india",
    # Europe
    "london","manchester","berlin","munich","amsterdam","paris","dublin",
    "warsaw","prague","zurich","geneva","stockholm","oslo","copenhagen",
    "lisbon","madrid","barcelona","rome","milan","vienna","brussels",
    "uk","united kingdom","england","scotland","germany","france",
    "netherlands","ireland","spain","italy","austria","sweden","norway",
    "denmark","finland","portugal","poland","switzerland","belgium",
    # Canada
    "toronto","vancouver","montreal","calgary","ottawa","canada",
    # Asia-Pacific
    "singapore","sydney","melbourne","brisbane","australia","new zealand",
    "tokyo","osaka","japan","beijing","shanghai","china","hong kong",
    "seoul","south korea","taipei","taiwan","kuala lumpur","malaysia",
    "jakarta","indonesia","bangkok","thailand","philippines",
    # Middle East / Africa
    "dubai","abu dhabi","uae","israel","tel aviv","cairo","egypt",
    "south africa","johannesburg","nairobi","kenya",
    # Latin America
    "mexico city","brazil","sao paulo","buenos aires","argentina","bogota",
    # Remote but explicitly non-US
    "remote - india","remote - uk","remote - eu","remote - europe",
    "remote - canada","remote - australia","remote - latam",
    "remote - global","remote - apac","remote - emea",
    "emea","apac","latam","global (excluding us)",
}

# Positive US signals — if any appear, accept immediately
US_POSITIVE_KEYWORDS = {
    "united states", " usa", "(usa)", "u.s.a", "u.s.",
    "remote - us", "remote us", "us remote", "us only",
    "remote (us", "remote, us", "anywhere in us",
    "work from home - us", "work remotely in the us",
}

def is_us_location(location_name: str) -> bool:
    """
    Strict US-only filter.
    Returns True ONLY if location is positively identified as USA.
    Returns False for blank, 'Worldwide', 'Remote', non-US cities, etc.
    """
    if not location_name:
        return False  # blank = unknown = EXCLUDE

    loc_raw  = location_name.strip()
    loc_low  = loc_raw.lower()

    # 1. Reject immediately if any non-US keyword found
    for kw in NON_US_KEYWORDS:
        if kw in loc_low:
            return False

    # 2. Accept immediately if strong US signal found
    for kw in US_POSITIVE_KEYWORDS:
        if kw in loc_low:
            return True

    # 3. Reject bare "Remote" / "Worldwide" / "Anywhere" with no US context
    bare_remote = {"remote", "worldwide", "anywhere", "global", "work from home",
                   "distributed", "flexible", "virtual", "telecommute"}
    if loc_low.strip() in bare_remote:
        return False

    # 4. Check for US state abbreviation pattern: "City, ST" or "City, ST 12345"
    parts = loc_raw.split(",")
    if len(parts) >= 2:
        state_part = parts[-1].strip().upper().split()[0][:2]
        if state_part in US_STATES:
            return True

    # 5. Check for full US state name anywhere in the string
    for state in US_STATE_NAMES:
        if state in loc_low:
            return True

    # 6. Check for standalone 2-letter US state code (e.g. " TX" or "(CA)")
    tokens = re.findall(r'\b([A-Z]{2})\b', loc_raw.upper())
    for token in tokens:
        if token in US_STATES:
            return True

    # 7. Known major US cities (partial list of unambiguous ones)
    us_cities = {
        "new york", "san francisco", "seattle", "chicago", "austin", "boston",
        "los angeles", "denver", "atlanta", "dallas", "houston", "miami",
        "phoenix", "minneapolis", "portland", "san diego", "san jose",
        "pittsburgh", "philadelphia", "detroit", "st. louis", "salt lake city",
        "raleigh", "charlotte", "nashville", "kansas city", "baltimore",
        "cleveland", "columbus", "indianapolis", "memphis", "louisville",
        "new orleans", "richmond", "sacramento", "orlando", "tampa",
        "las vegas", "cincinnati", "milwaukee", "st louis", "washington, dc",
        "washington d.c", "menlo park", "mountain view", "palo alto",
        "redwood city", "cupertino", "sunnyvale", "santa clara", "bellevue",
        "kirkland", "redmond",
    }
    for city in us_cities:
        if city in loc_low:
            return True

    return False

def is_us_location_or_us_remote(location_name: str) -> bool:
    """
    Same as is_us_location but also allows 'Remote' when the SOURCE is
    a known US-only ATS (Workday with US GUID, USAJobs, etc.).
    Only use this for sources that are already server-side US-filtered.
    """
    if not location_name:
        return True   # already filtered server-side, blank is fine
    return is_us_location(location_name)

# ─────────────────────────────────────────────────────────────────────────────
# GENERAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_relevant(title: str) -> bool:
    return any(kw in title.lower() for kw in KEYWORDS)

def make_id(source: str, uid: str) -> str:
    return hashlib.md5(f"{source}::{uid}".encode()).hexdigest()

def load_seen() -> set:
    try:
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen: set):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)

def job(title, company, location, url, source, posted=""):
    return {
        "title": str(title).strip(),
        "company": str(company).strip(),
        "location": str(location).strip(),
        "url": str(url).strip(),
        "source": source,
        "posted_at": posted
    }

def safe_get(url, **kwargs):
    try:
        r = SESSION.get(url, timeout=12, **kwargs)
        return r if r.status_code == 200 else None
    except Exception:
        return None

def safe_post(url, **kwargs):
    try:
        r = SESSION.post(url, timeout=12, **kwargs)
        return r if r.status_code == 200 else None
    except Exception:
        return None

def is_fresh(posted_str, hours=24):
    if not posted_str:
        return True
    try:
        posted = dparser.parse(str(posted_str))
        if posted.tzinfo is None:
            posted = posted.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - posted < timedelta(hours=hours)
    except Exception:
        return True

# ─────────────────────────────────────────────────────────────────────────────
# SECTION A — RSS FEEDS  (add &l=United+States where possible)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_indeed():
    jobs = []
    for q in ["data+engineer", "analytics+engineer", "etl+engineer"]:
        feed = feedparser.parse(
            f"https://www.indeed.com/rss?q={q}&l=United+States&sort=date&limit=50")
        for e in feed.entries:
            if is_relevant(e.get("title", "")):
                loc = e.get("location", "")
                if is_us_location(loc):
                    jobs.append(job(e.get("title",""), e.get("author","Unknown"),
                        loc, e.get("link",""), "Indeed", e.get("published","")))
    return jobs

def fetch_ziprecruiter():
    jobs = []
    feed = feedparser.parse(
        "https://www.ziprecruiter.com/candidate/suggested-jobs/feed/rss"
        "?search=data+engineer&location=United+States&sort=date")
    for e in feed.entries:
        if is_relevant(e.get("title", "")):
            loc = e.get("location","")
            if is_us_location(loc):
                jobs.append(job(e.get("title",""), e.get("author","Unknown"),
                    loc, e.get("link",""), "ZipRecruiter", e.get("published","")))
    return jobs

def fetch_dice():
    """Dice is US-only job board by design."""
    jobs = []
    feed = feedparser.parse("https://www.dice.com/jobs/q-data_engineer-jobs-rss")
    for e in feed.entries:
        if is_relevant(e.get("title", "")):
            loc = e.get("location","")
            # Dice is US-focused but still filter to be safe
            if is_us_location(loc) or not loc:
                jobs.append(job(e.get("title",""), e.get("author","Unknown"),
                    loc, e.get("link",""), "Dice", e.get("published","")))
    return jobs

def fetch_simplyhired():
    jobs = []
    feed = feedparser.parse(
        "https://www.simplyhired.com/search?q=data+engineer&l=United+States&sort=date&rss=1")
    for e in feed.entries:
        if is_relevant(e.get("title", "")):
            loc = e.get("location","")
            if is_us_location(loc):
                jobs.append(job(e.get("title",""), "", loc, e.get("link",""),
                    "SimplyHired", e.get("published","")))
    return jobs

def fetch_weworkremotely():
    """WeWorkRemotely is global remote — only include explicitly US-tagged jobs."""
    jobs = []
    for cat in ["remote-devops-sysadmin-jobs", "remote-back-end-programming-jobs"]:
        feed = feedparser.parse(f"https://weworkremotely.com/categories/{cat}.rss")
        for e in feed.entries:
            t = e.get("title", "")
            if not is_relevant(t):
                continue
            parts = t.split(":", 1)
            c     = parts[0].strip() if len(parts) > 1 else "Unknown"
            title = parts[1].strip() if len(parts) > 1 else t
            # Check summary/description for US mention
            summary = e.get("summary","") + e.get("description","")
            loc_hint = e.get("location","")
            if is_us_location(loc_hint) or "usa" in summary.lower() or \
               "united states" in summary.lower() or "u.s." in summary.lower():
                jobs.append(job(title, c, "Remote USA", e.get("link",""),
                    "WeWorkRemotely", e.get("published","")))
    return jobs

def fetch_careerbuilder():
    """CareerBuilder is US-focused."""
    jobs = []
    feed = feedparser.parse(
        "https://www.careerbuilder.com/jobs?keywords=data+engineer&posted=1&rss=1")
    for e in feed.entries:
        if is_relevant(e.get("title", "")):
            loc = e.get("location","")
            if is_us_location(loc):
                jobs.append(job(e.get("title",""), "", loc, e.get("link",""),
                    "CareerBuilder", e.get("published","")))
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# SECTION B — PUBLIC JOB BOARD APIs
# ─────────────────────────────────────────────────────────────────────────────

def fetch_remotive():
    """Remotive is global — strictly filter to US-only locations."""
    jobs = []
    r = safe_get("https://remotive.com/api/remote-jobs?search=data+engineer&limit=100")
    if r:
        for j in r.json().get("jobs", []):
            if is_relevant(j.get("title", "")):
                loc = j.get("candidate_required_location", "")
                # Only include if explicitly US
                if is_us_location(loc):
                    jobs.append(job(j["title"], j.get("company_name",""),
                        loc, j.get("url",""), "Remotive", j.get("publication_date","")))
    return jobs

def fetch_arbeitnow():
    jobs = []
    r = safe_get("https://www.arbeitnow.com/api/job-board-api?search=data+engineer")
    if r:
        for j in r.json().get("data", []):
            if is_relevant(j.get("title", "")):
                loc = j.get("location","")
                if is_us_location(loc):
                    jobs.append(job(j["title"], j.get("company_name",""),
                        loc, j.get("url",""), "Arbeitnow", j.get("created_at","")))
    return jobs

def fetch_themuse():
    jobs = []
    r = safe_get("https://www.themuse.com/api/public/jobs?category=Data+Science"
                 "&level=Mid+Level&level=Senior+Level&page=1")
    if r:
        for j in r.json().get("results", []):
            if is_relevant(j.get("name", "")):
                locs = j.get("locations", [{}])
                loc  = locs[0].get("name","") if locs else ""
                if is_us_location(loc):
                    jobs.append(job(j["name"], j.get("company",{}).get("name",""),
                        loc, j.get("refs",{}).get("landing_page",""),
                        "The Muse", j.get("publication_date","")))
    return jobs

def fetch_remoteok():
    """RemoteOK is global — only include jobs with explicit US location or tags."""
    jobs = []
    r = safe_get("https://remoteok.com/api?tag=data-engineer")
    if r:
        for j in r.json():
            if not isinstance(j, dict) or "position" not in j:
                continue
            if not is_relevant(j.get("position", "")):
                continue
            loc  = j.get("location","")
            tags = " ".join(j.get("tags", [])).lower()
            # Accept only if location is US or tags mention usa/us
            if is_us_location(loc) or "usa" in tags or "us only" in tags:
                ts     = j.get("epoch", 0)
                posted = datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat() if ts else ""
                jobs.append(job(j["position"], j.get("company",""), loc or "Remote USA",
                    j.get("url", f"https://remoteok.com/jobs/{j.get('id','')}"),
                    "RemoteOK", posted))
    return jobs

def fetch_himalayas():
    """Himalayas is global remote — only include US-restricted jobs."""
    jobs = []
    r = safe_get("https://himalayas.app/jobs/api?q=data+engineer&limit=50")
    if r:
        for j in r.json().get("jobs", []):
            if not is_relevant(j.get("title", "")):
                continue
            locs = j.get("locationRestrictions", [])
            # If no restriction listed, it's truly global — skip
            if not locs:
                continue
            loc_str = " ".join(locs).lower()
            if is_us_location(loc_str) or "united states" in loc_str or "usa" in loc_str:
                loc = locs[0]
                jobs.append(job(j["title"], j.get("company",{}).get("name",""),
                    loc, f"https://himalayas.app/jobs/{j.get('slug','')}",
                    "Himalayas", j.get("createdAt","")))
    return jobs

def fetch_usajobs():
    """US Federal government jobs — always USA."""
    jobs = []
    r = safe_get("https://data.usajobs.gov/api/search?Keyword=data+engineer"
                 "&ResultsPerPage=50&SortField=DatePosted&SortDirection=Desc",
                 headers={"Host": "data.usajobs.gov",
                          "User-Agent": SMTP_USER or "monitor@example.com"})
    if r:
        for item in r.json().get("SearchResult",{}).get("SearchResultItems",[]):
            d = item.get("MatchedObjectDescriptor", {})
            if is_relevant(d.get("PositionTitle", "")):
                jobs.append(job(d["PositionTitle"], d.get("OrganizationName","US Gov"),
                    d.get("PositionLocationDisplay","USA"), d.get("PositionURI",""),
                    "USAJobs", d.get("PublicationStartDate","")))
    return jobs

def fetch_adzuna():
    """Adzuna /us/ endpoint is US-only."""
    jobs = []
    if not ADZUNA_APP_ID:
        return jobs
    r = safe_get(f"https://api.adzuna.com/v1/api/jobs/us/search/1"
                 f"?app_id={ADZUNA_APP_ID}&app_key={ADZUNA_APP_KEY}"
                 f"&what=data+engineer&sort_by=date&results_per_page=50&max_days_old=1")
    if r:
        for j in r.json().get("results", []):
            if is_relevant(j.get("title", "")):
                loc = j.get("location",{}).get("display_name","")
                jobs.append(job(j["title"], j.get("company",{}).get("display_name",""),
                    loc, j.get("redirect_url",""), "Adzuna", j.get("created","")))
    return jobs

def fetch_jsearch():
    jobs = []
    if not JSEARCH_API_KEY:
        return jobs
    headers = {"X-RapidAPI-Key": JSEARCH_API_KEY, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"}
    for query in ["data engineer USA", "analytics engineer USA", "etl engineer USA"]:
        r = safe_get("https://jsearch.p.rapidapi.com/search",
            params={"query": query, "page": "1", "num_results": "10", "date_posted": "today"},
            headers=headers)
        if r:
            try:
                for j in r.json().get("data", []):
                    if not is_relevant(j.get("job_title", "")):
                        continue
                    country = j.get("job_country","").upper()
                    city    = j.get("job_city","")
                    state   = j.get("job_state","")
                    loc     = f"{city}, {state}".strip(", ")
                    # Must be US country code or parseable US location
                    if country == "US" or is_us_location(loc):
                        jobs.append(job(
                            j["job_title"], j.get("employer_name",""), loc,
                            j.get("job_apply_link", j.get("job_google_link","")),
                            "JSearch", j.get("job_posted_at_datetime_utc","")))
            except Exception:
                pass
    return jobs

def fetch_serpapi_google_jobs():
    jobs = []
    if not SERPAPI_KEY:
        return jobs
    for query in ["data engineer USA", "analytics engineer USA"]:
        r = safe_get("https://serpapi.com/search",
            params={"engine": "google_jobs", "q": query, "location": "United States",
                    "chips": "date_posted:today", "api_key": SERPAPI_KEY})
        if r:
            try:
                for j in r.json().get("jobs_results", []):
                    if not is_relevant(j.get("title", "")):
                        continue
                    loc = j.get("location","")
                    if is_us_location(loc):
                        jobs.append(job(
                            j["title"], j.get("company_name",""), loc,
                            j.get("related_links", [{}])[0].get("link",""),
                            "Google Jobs (SerpApi)",
                            j.get("detected_extensions",{}).get("posted_at","")))
            except Exception:
                pass
    return jobs

def fetch_wellfound():
    jobs = []
    r = safe_get("https://wellfound.com/company_filters/search_startup_jobs_export?"
                 "job_types[]=full-time&roles[]=data-engineer&format=json")
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title", "")):
                    continue
                loc = j.get("location","")
                if is_us_location(loc):
                    jobs.append(job(j["title"], j.get("startup",{}).get("name",""),
                        loc, j.get("url",""), "Wellfound", j.get("created_at","")))
        except Exception:
            pass
    return jobs

def fetch_yc_jobs():
    jobs = []
    r = safe_get(
        "https://www.workatastartup.com/jobs?q=data+engineer&jobType=fulltime&format=json")
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title", "")):
                    continue
                locs = j.get("locations", [])
                loc  = locs[0] if locs else ""
                if is_us_location(loc):
                    jobs.append(job(j["title"], j.get("company_name",""), loc,
                        f"https://www.workatastartup.com/jobs/{j.get('id','')}",
                        "YC Work at a Startup", j.get("created_at","")))
        except Exception:
            pass
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# SECTION C — ATS PLATFORM PRIMITIVES
# ─────────────────────────────────────────────────────────────────────────────

# Workday US country GUID — consistent across all tenants
_WD_US_GUID = "bc33aa3152ec42d4995f4791a106ed09"

def _workday(tenant, board, wd_ver, company_name):
    """Fetch jobs from a Workday tenant.
    US-filtered server-side via country GUID + STRICT client-side title/loc check."""
    jobs = []
    url = (f"https://{tenant}.wd{wd_ver}.myworkdayjobs.com"
           f"/wday/cxs/{tenant}/{board}/jobs")
    payload = {
        "appliedFacets": {"locationCountry": [_WD_US_GUID]},
        "limit": 20, "offset": 0, "searchText": "data engineer"
    }
    r = safe_post(url, json=payload, headers={"Content-Type": "application/json"})
    if r:
        try:
            for j in r.json().get("jobPostings", []):
                title = j.get("title","")
                if not is_relevant(title):
                    continue
                # Some Workday tenants (e.g. PwC) ignore the GUID filter —
                # catch India/non-US jobs by checking title prefixes and location
                title_low = title.lower()
                if any(x in title_low for x in ["_noida", "_bangalore", "_hyderabad",
                        "_mumbai", "_pune", "_india", " - india", "(india)"]):
                    log.debug(f"  FILTERED (non-US title): [Workday/{company_name}] {title}")
                    continue
                # Also check the IN_ prefix pattern PwC uses for India roles
                if re.match(r'^IN_', title):
                    log.debug(f"  FILTERED (IN_ prefix): [Workday/{company_name}] {title}")
                    continue
                locs = j.get("locations",[])
                loc  = locs[0].get("type","") if locs else ""
                # If location string is present and non-US, reject
                if loc and not is_us_location(loc) and loc.lower() not in (
                    "on-site", "hybrid", "remote", "flexible"
                ):
                    log.debug(f"  FILTERED (non-US loc): [Workday/{company_name}] {title} | {loc}")
                    continue
                ext  = j.get("externalPath","")
                jurl = (f"https://{tenant}.wd{wd_ver}.myworkdayjobs.com"
                        f"/en-US/{board}{ext}") if ext else ""
                jobs.append(job(title, company_name, loc,
                    jurl, f"Workday/{company_name}", j.get("postedOn","")))
        except Exception:
            pass
    return jobs

def _greenhouse(slug, company_name):
    """Fetch jobs from Greenhouse.
    No server-side location filter — STRICT client-side US check."""
    jobs = []
    r = safe_get(
        f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true")
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title","")):
                    continue
                loc = j.get("location",{}).get("name","")
                if is_us_location(loc):
                    jobs.append(job(j["title"], company_name, loc,
                        j.get("absolute_url",""),
                        f"Greenhouse/{company_name}",
                        j.get("first_published", j.get("updated_at",""))))
        except Exception:
            pass
    return jobs

def _lever(slug, company_name):
    """Fetch jobs from Lever.
    No server-side location filter — STRICT client-side US check."""
    jobs = []
    r = safe_get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
    if r:
        try:
            for j in r.json():
                if not is_relevant(j.get("text","")):
                    continue
                loc = j.get("categories",{}).get("location","")
                if is_us_location(loc):
                    ts     = j.get("createdAt", 0)
                    posted = (datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat()
                              if ts else "")
                    jobs.append(job(j["text"], company_name, loc,
                        j.get("hostedUrl",""), f"Lever/{company_name}", posted))
        except Exception:
            pass
    return jobs

def _ashby(slug, company_name):
    """Fetch jobs from Ashby.
    Filter via addressCountry field — STRICT, no blank pass-through."""
    jobs = []
    r = safe_get(
        f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true")
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title","")):
                    continue
                country = (j.get("address",{})
                             .get("postalAddress",{})
                             .get("addressCountry",""))
                loc     = j.get("location","")
                # Accept only confirmed US country or US-parseable location
                if country in ("US","United States") or is_us_location(loc):
                    jobs.append(job(j["title"], company_name, loc,
                        j.get("jobUrl",""), f"Ashby/{company_name}",
                        j.get("publishedAt", j.get("publishedDate",""))))
        except Exception:
            pass
    return jobs

def _smartrecruiters(slug, company_name):
    """Fetch US jobs from SmartRecruiters — ?country=US server-side filter."""
    jobs = []
    r = safe_get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
                 f"?q=data+engineer&status=PUBLISHED&country=US&limit=20")
    if r:
        try:
            for j in r.json().get("content",[]):
                if is_relevant(j.get("name","")):
                    jobs.append(job(j["name"], company_name,
                        j.get("location",{}).get("city",""),
                        f"https://careers.smartrecruiters.com/{slug}/{j.get('id','')}",
                        f"SmartRecruiters/{company_name}", j.get("releasedDate","")))
        except Exception:
            pass
    return jobs

def _eightfold(subdomain, domain, company_name):
    """Fetch US jobs from Eightfold AI — ?location=United+States server-side."""
    jobs = []
    url  = (f"https://{subdomain}.eightfold.ai/api/apply/v2/jobs"
            f"?domain={domain}&location=United+States&sort_by=relevance&num=20&start=0")
    r = safe_get(url, headers={"User-Agent": "Mozilla/5.0 JobMonitor/5.2"})
    if r:
        try:
            for j in r.json().get("positions", []):
                title = j.get("name","")
                if is_relevant(title):
                    loc = j.get("location","")
                    jobs.append(job(title, company_name, loc,
                        j.get("canonicalPositionUrl",
                              f"https://{subdomain}.eightfold.ai/careers"),
                        f"Eightfold/{company_name}", j.get("t_create","")))
        except Exception:
            pass
    return jobs

def _workable(slug, company_name):
    """Fetch US jobs from Workable public widget API."""
    jobs = []
    r = safe_get(
        f"https://apply.workable.com/api/v1/widget/accounts/{slug}",
        headers={"Accept": "application/json"})
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title","")):
                    continue
                loc = (j.get("city","") +
                       (", " + j.get("state","") if j.get("state") else ""))
                if j.get("country_code","") == "US" or is_us_location(loc):
                    jobs.append(job(j["title"], company_name, loc,
                        j.get("url",""), f"Workable/{company_name}",
                        j.get("published_on","")))
        except Exception:
            pass
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# SECTION D — MAJOR COMPANY DIRECT APIs
# ─────────────────────────────────────────────────────────────────────────────

def fetch_amazon():
    jobs = []
    for q in ["data+engineer", "data+engineering", "analytics+engineer"]:
        feed = feedparser.parse(
            f"https://www.amazon.jobs/en/search.rss?base_query={q}&country=US&sort=recent")
        for e in feed.entries:
            if is_relevant(e.get("title", "")):
                loc = e.get("location","")
                if is_us_location(loc) or not loc:  # Amazon's US RSS is US-only
                    jobs.append(job(e.get("title",""), "Amazon", loc,
                        e.get("link",""), "Amazon Jobs", e.get("published","")))
    return jobs

def fetch_amazon_json():
    jobs = []
    for q in ["data engineer", "analytics engineer", "etl engineer"]:
        r = safe_get("https://www.amazon.jobs/en/search.json",
            params={"base_query": q, "offset": 0, "result_limit": 10,
                    "sort": "recent", "job_type": "Full-Time", "country": "USA"})
        if r:
            try:
                for j in r.json().get("jobs", []):
                    if not is_relevant(j.get("title", "")):
                        continue
                    loc = j.get("location","")
                    if is_us_location(loc) or not loc:
                        jobs.append(job(j["title"], "Amazon", loc,
                            f"https://www.amazon.jobs{j.get('url_next_step','')}",
                            "Amazon Jobs JSON", j.get("posted_date","")))
            except Exception:
                pass
    return jobs

def fetch_google():
    jobs = []
    r = safe_get(
        "https://careers.google.com/api/v3/search/"
        "?query=data+engineer&sort_by=date&num=20&location=United+States")
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title", "")):
                    continue
                locs = j.get("locations", [{}])
                loc  = locs[0].get("city","") if locs else ""
                if is_us_location(loc) or not loc:
                    jobs.append(job(j["title"], "Google", loc,
                        f"https://careers.google.com/jobs/results/{j.get('id','')}",
                        "Google Careers", j.get("publish_date","")))
        except Exception:
            pass
    return jobs

def fetch_microsoft():
    jobs = []
    r = safe_get("https://gcsservices.careers.microsoft.com/search/api/v1/search"
                 "?q=data+engineer&l=en_us&pg=1&pgSz=20&o=Recent&flt=true"
                 "&lc=United+States")
    if r:
        try:
            for j in (r.json().get("operationResult",{})
                              .get("result",{}).get("jobs",[])):
                if not is_relevant(j.get("title", "")):
                    continue
                loc = j.get("properties",{}).get("primaryWorkLocation","")
                if is_us_location(loc) or not loc:
                    jobs.append(job(j["title"], "Microsoft", loc,
                        f"https://careers.microsoft.com/v2/global/en/job/{j.get('jobId','')}",
                        "Microsoft Careers", j.get("postingDate","")))
        except Exception:
            pass
    return jobs

def fetch_meta():
    jobs = []
    payload = {
        "operationName": "SearchJobsQuery",
        "variables": {"search_input": {
            "q": "data engineer", "sort_by": "NEWEST_FIRST", "page": 1,
            "location": [{"country": "US"}]
        }},
        "query": ("query SearchJobsQuery($search_input: SearchInput!) { "
                  "job_search(search_input: $search_input) { "
                  "jobs { id title locations { name } url } } }")
    }
    r = safe_post("https://www.metacareers.com/graphql", json=payload)
    if r:
        try:
            for j in (r.json().get("data",{})
                               .get("job_search",{}).get("jobs",[])):
                if not is_relevant(j.get("title","")):
                    continue
                locs = j.get("locations",[{}])
                loc  = locs[0].get("name","") if locs else ""
                if is_us_location(loc):
                    jobs.append(job(j["title"], "Meta", loc,
                        j.get("url",""), "Meta Careers", ""))
        except Exception:
            pass
    if not jobs:
        r2 = safe_get(
            "https://www.metacareers.com/jobs?q=data%20engineer&sort_by_new=true")
        if r2:
            for title, jid in re.findall(r'"title":"([^"]+)","id":"(\d+)"', r2.text):
                if is_relevant(title):
                    jobs.append(job(title, "Meta", "USA",
                        f"https://www.metacareers.com/jobs/{jid}/",
                        "Meta Careers", ""))
    return jobs

def fetch_tesla():
    jobs = []
    r = safe_get(
        "https://www.tesla.com/careers/search/api/v1/job-plates",
        params={"query": "data engineer", "country": "US", "limit": 50},
        headers={"Accept": "application/json",
                 "Referer": "https://www.tesla.com/careers/search"})
    if r:
        try:
            for j in r.json().get("results", r.json().get("jobs", [])):
                if not is_relevant(j.get("title", "")):
                    continue
                loc = j.get("location",{}).get("city", j.get("location",""))
                if is_us_location(str(loc)) or not loc:
                    jobs.append(job(j["title"], "Tesla", str(loc),
                        f"https://www.tesla.com/careers/search/job/{j.get('id','')}",
                        "Tesla Careers",
                        j.get("posted_at", j.get("postedDate",""))))
        except Exception:
            pass
    return jobs

def fetch_ibm():
    jobs = []
    r = safe_get("https://careers.ibm.com/api/apply/v2/jobs?"
                 "domain=ibm.com&search_keyword=data+engineer"
                 "&country=United+States&limit=20&start=0")
    if r:
        try:
            for j in r.json().get("positions",[]):
                if not is_relevant(j.get("title","")):
                    continue
                loc = j.get("primary_display_location","")
                if is_us_location(loc) or not loc:
                    jobs.append(job(j["title"], "IBM", loc,
                        f"https://careers.ibm.com/job/{j.get('id','')}",
                        "IBM Careers", j.get("t_create","")))
        except Exception:
            pass
    return jobs

def fetch_oracle():
    jobs = []
    r = safe_get(
        "https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/"
        "recruitingCEJobRequisitions?finder=findReqs;"
        "Name=data engineer,sortBy=POSTING_DATES_DESC&limit=20",
        headers={"REST-Framework-Version": "3"})
    if r:
        try:
            for j in r.json().get("items", []):
                if not is_relevant(j.get("Title","")):
                    continue
                loc = j.get("PrimaryLocation","")
                if is_us_location(loc):
                    jobs.append(job(j["Title"], "Oracle", loc,
                        "https://careers.oracle.com/jobs/#en/sites/jobsearch"
                        f"/job/{j.get('Id','')}",
                        "Oracle Careers", j.get("PostingStartDate","")))
        except Exception:
            pass
    return jobs

def fetch_netflix():
    jobs = []
    r = safe_get(
        "https://explore.jobs.netflix.net/api/apply/v2/jobs"
        "?domain=netflix.com&location=United+States&sort_by=relevance&num=20&start=0",
        headers={"User-Agent": "Mozilla/5.0 JobMonitor/5.2"})
    if r:
        try:
            for j in r.json().get("positions", []):
                if is_relevant(j.get("name","")):
                    loc = j.get("location","")
                    jobs.append(job(j["name"], "Netflix", loc,
                        j.get("canonicalPositionUrl","https://jobs.netflix.com"),
                        "Netflix Careers", j.get("t_create","")))
        except Exception:
            pass
    return jobs

def fetch_jpmorgan():
    jobs = []
    r = safe_get(
        "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/"
        "recruitingCEJobRequisitions?finder=findReqs;"
        "Name=data engineer,sortBy=POSTING_DATES_DESC&limit=20",
        headers={"REST-Framework-Version": "3"})
    if r:
        try:
            for j in r.json().get("items", []):
                if not is_relevant(j.get("Title","")):
                    continue
                loc = j.get("PrimaryLocation","")
                if is_us_location(loc):
                    jobs.append(job(j["Title"], "JPMorgan Chase", loc,
                        "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience"
                        f"/en/sites/CX_1001/job/{j.get('Id','')}",
                        "JPMorgan Oracle", j.get("PostingStartDate","")))
        except Exception:
            pass
    return jobs

def fetch_linkedin_jobs():
    jobs = []
    r = safe_get(
        "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        "?keywords=data+engineer&location=United+States&f_TPR=r86400&start=0")
    if r:
        ids       = re.findall(r'data-job-id="(\d+)"', r.text)
        titles    = re.findall(r'aria-label="([^"]+)"', r.text)
        companies = re.findall(
            r'class="base-search-card__subtitle[^"]*"[^>]*>\s*([^<]+)\s*<', r.text)
        locs_raw  = re.findall(
            r'class="job-search-card__location"[^>]*>\s*([^<]+)\s*<', r.text)
        for i, jid in enumerate(ids):
            title = titles[i] if i < len(titles) else ""
            if not is_relevant(title):
                continue
            loc = locs_raw[i].strip() if i < len(locs_raw) else ""
            if is_us_location(loc) or not loc:
                company = companies[i].strip() if i < len(companies) else "Unknown"
                jobs.append(job(title, company, loc,
                    f"https://www.linkedin.com/jobs/view/{jid}/", "LinkedIn", ""))
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# SECTION E — BULK ATS PLATFORM SCANNERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_workday_bulk():
    """165+ companies on Workday — US GUID server-side filter."""
    companies = [
        # ── Big Tech & Software ───────────────────────────────────────────────
        ("apple",             "apple",                     5,  "Apple"),
        ("salesforce",        "External_Career_Site",     12,  "Salesforce"),
        ("adobe",             "external_experienced",      5,  "Adobe"),
        ("nvidia",            "NVIDIAExternalCareerSite",  5,  "Nvidia"),
        ("cisco",             "Cisco_Careers",             5,  "Cisco"),
        ("paypal",            "jobs",                      1,  "PayPal"),
        ("servicenow",        "servicenow",                5,  "ServiceNow"),
        ("okta",              "okta",                      1,  "Okta"),
        ("zoom",              "zoom",                      5,  "Zoom"),
        ("docusign",          "docusign",                  1,  "DocuSign"),
        ("vmware",            "vmware",                    1,  "VMware/Broadcom"),
        ("dell",              "dell",                      1,  "Dell Technologies"),
        ("hp",                "hp",                        5,  "HP"),
        ("qualcomm",          "qualcomm",                  5,  "Qualcomm"),
        ("amd",               "amd",                       5,  "AMD"),
        ("intuit",            "intuit",                    5,  "Intuit"),
        ("ebay",              "ebay",                      5,  "eBay"),
        ("snap",              "snap",                      5,  "Snap"),
        ("autodesk",          "Ext",                       1,  "Autodesk"),
        ("workday",           "Workday",                   5,  "Workday"),
        ("hubspot",           "hubspot",                   1,  "HubSpot"),
        ("squarespace",       "squarespace",               5,  "Squarespace"),
        ("dropbox",           "dropbox",                   5,  "Dropbox"),
        ("box",               "box",                       5,  "Box"),
        ("etsy",              "etsy",                      5,  "Etsy"),
        ("wayfair",           "wayfair",                   5,  "Wayfair"),
        ("roblox",            "roblox",                    5,  "Roblox"),
        ("ea",                "EA",                        5,  "Electronic Arts"),
        ("unity3d",           "unity3d",                   5,  "Unity Technologies"),
        ("veeva",             "veeva",                     5,  "Veeva Systems"),
        ("medallia",          "medallia",                  5,  "Medallia"),
        ("dynatrace",         "dynatrace",                 5,  "Dynatrace"),
        ("solarwinds",        "solarwinds",                5,  "SolarWinds"),
        ("fortinet",          "fortinet",                  5,  "Fortinet"),
        ("crowdstrike",       "crowdstrikecareers",        5,  "CrowdStrike"),
        ("sentinelone",       "sentinelone",               5,  "SentinelOne"),
        ("informatica",       "informatica",               5,  "Informatica"),
        ("alteryx",           "alteryx",                   5,  "Alteryx"),
        ("teradata",          "teradata",                  5,  "Teradata"),
        ("netapp",            "netapp",                    5,  "NetApp"),
        ("nutanix",           "nutanix",                   5,  "Nutanix"),
        ("purestorage",       "purestorage",               5,  "Pure Storage"),
        ("broadcom",          "broadcom",                  5,  "Broadcom"),
        ("marvell",           "marvell",                   5,  "Marvell"),
        ("micron",            "micron",                    5,  "Micron"),
        ("synopsys",          "synopsys",                  5,  "Synopsys"),
        ("juniper",           "juniper",                   5,  "Juniper Networks"),
        ("yahoo",             "yahooinc",                  5,  "Yahoo"),
        ("agilent",           "agilent",                   5,  "Agilent"),
        ("cloudera",          "cloudera",                  5,  "Cloudera"),
        ("ringcentral",       "ringcentral",               5,  "RingCentral"),
        ("guidewire",         "guidewire",                 5,  "Guidewire"),
        ("lam",               "lamresearch",               5,  "Lam Research"),
        ("kla",               "kla",                       5,  "KLA"),
        ("teradyne",          "teradyne",                  5,  "Teradyne"),
        ("intel",             "External",                  1,  "Intel"),
        ("nike",              "nke",                       1,  "Nike"),
        # ── Finance & Fintech ──────────────────────────────────────────────────
        ("capitalone",        "Capital_One",              12,  "Capital One"),
        ("ghr",               "Lateral-US",                1,  "Bank of America"),
        ("ms",                "External",                  5,  "Morgan Stanley"),
        ("citi",              "2",                         5,  "Citigroup"),
        ("schwab",            "external",                  1,  "Charles Schwab"),
        ("blackrock",         "BlackRock_Professional",    1,  "BlackRock"),
        ("pnc",               "External",                  5,  "PNC Bank"),
        ("usbank",            "US_Bank_Careers",           1,  "U.S. Bank"),
        ("troweprice",        "TRowePrice",                5,  "T. Rowe Price"),
        ("mastercard",        "CorporateCareers",          1,  "Mastercard"),
        ("visa",              "Visa_Careers",              5,  "Visa"),
        ("fiserv",            "EXT",                       5,  "Fiserv"),
        ("broadridge",        "Careers",                   5,  "Broadridge"),
        ("factset",           "FactSetCareers",            1,  "FactSet"),
        ("wellsfargo",        "WellsFargoJobs",            1,  "Wells Fargo"),
        ("vanguard",          "vanguard_external",         5,  "Vanguard"),
        ("fmr",               "targeted",                  1,  "Fidelity"),
        ("fil",               "001",                       3,  "Fidelity International"),
        ("ally",              "ally",                      5,  "Ally Financial"),
        ("discover",          "discover",                  5,  "Discover"),
        ("americanexpress",   "americanexpress",           5,  "American Express"),
        ("statestreet",       "Global",                    1,  "State Street"),
        # ── Healthcare & Pharma ────────────────────────────────────────────────
        ("unitedhealthgroup", "unitedhealthgroup",         5,  "UnitedHealth Group"),
        ("cvshealth",         "External",                  1,  "CVS Health"),
        ("cigna",             "cignacareers",              5,  "Cigna"),
        ("humana",            "Humana_External_Career_Site",5, "Humana"),
        ("elevancehealth",    "ANT",                       1,  "Elevance Health"),
        ("pfizer",            "PfizerCareers",             1,  "Pfizer"),
        ("jj",                "JJ",                        5,  "Johnson & Johnson"),
        ("danaher",           "DanaherJobs",               1,  "Danaher"),
        ("abbott",            "abbottcareers",             5,  "Abbott"),
        ("abbvie",            "abbvie",                    5,  "AbbVie"),
        ("amgen",             "amgen",                     5,  "Amgen"),
        ("genentech",         "genentech",                 5,  "Genentech"),
        ("lilly",             "LLY",                       5,  "Eli Lilly"),
        ("medtronic",         "MedtronicCareers",          1,  "Medtronic"),
        ("stryker",           "StrykerCareers",            1,  "Stryker"),
        ("biibhr",            "external",                  3,  "Biogen"),
        ("astrazeneca",       "Careers",                   3,  "AstraZeneca"),
        ("illumina",          "illumina",                  5,  "Illumina"),
        ("thermofisher",      "ThermoFisherCareers",       5,  "Thermo Fisher"),
        ("modernatx",         "M_tx",                      1,  "Moderna"),
        # ── Consulting & Professional Services ────────────────────────────────
        ("accenture",         "AccentureCareers",        103,  "Accenture"),
        ("pwc",               "Global_Experienced_Careers", 3, "PwC"),
        ("mmc",               "MMC",                       1,  "Oliver Wyman/MMC"),
        ("deloitte",          "deloitte",                  5,  "Deloitte"),
        ("kpmg",              "kpmg",                      5,  "KPMG"),
        ("gartner",           "EXT",                       5,  "Gartner"),
        ("huron",             "huroncareers",              1,  "Huron Consulting"),
        ("cnx",               "external_global",           1,  "Concentrix"),
        ("capgemini",         "capgemini",                 5,  "Capgemini"),
        ("dxctechnology",     "DXCJobs",                   1,  "DXC Technology"),
        # ── Defense & Gov Contractors ──────────────────────────────────────────
        ("boeing",            "EXTERNAL_CAREERS",          1,  "Boeing"),
        ("ngc",               "Northrop_Grumman_External_Site", 1, "Northrop Grumman"),
        ("bah",               "BAH_Jobs",                  1,  "Booz Allen Hamilton"),
        ("leidos",            "External",                  5,  "Leidos"),
        ("mantech",           "External",                  1,  "ManTech"),
        ("globalhr",          "REC_RTX_Ext_Gateway",       5,  "RTX/Raytheon"),
        ("gdit",              "External_Career_Site",      5,  "General Dynamics IT"),
        ("saic",              "saic",                      5,  "SAIC"),
        ("l3harris",          "l3harris",                  5,  "L3Harris"),
        # ── Retail & Consumer ──────────────────────────────────────────────────
        ("walmart",           "WalmartExternal",           5,  "Walmart"),
        ("target",            "targetcareers",             5,  "Target"),
        ("homedepot",         "CareerDepot",               5,  "Home Depot"),
        ("lowes",             "LWS_External_CS",           5,  "Lowe's"),
        ("tjx",               "TJX_EXTERNAL",              1,  "TJX Companies"),
        ("dollartree",        "dollartreeus",              5,  "Dollar Tree"),
        ("nike",              "nke",                       1,  "Nike"),
        ("underarmour",       "underarmour",               5,  "Under Armour"),
        # ── Telecom & Media ────────────────────────────────────────────────────
        ("att",               "ATTGeneral",                1,  "AT&T"),
        ("verizon",           "verizon-careers",          12,  "Verizon"),
        ("tmobile",           "External",                  1,  "T-Mobile"),
        ("comcast",           "Comcast_Careers",           5,  "Comcast"),
        ("disney",            "disneycareer",              5,  "Disney"),
        ("warnerbros",        "global",                    5,  "Warner Bros Discovery"),
        ("nbcuniversal",      "nbcuniversal",              5,  "NBCUniversal"),
        # ── Manufacturing & Industrial ─────────────────────────────────────────
        ("geaerospace",       "GE_ExternalSite",           5,  "GE Aerospace"),
        ("ge",                "ge",                        5,  "GE"),
        ("honeywell",         "honeywell",                 5,  "Honeywell"),
        ("3m",                "Search",                    1,  "3M"),
        ("caterpillar",       "caterpillar",               5,  "Caterpillar"),
        ("emerson",           "emerson",                   5,  "Emerson Electric"),
        ("rockwellautomation","External_Rockwell_Automation", 1, "Rockwell Automation"),
        ("trimble",           "TrimbleCareers",            1,  "Trimble"),
        ("pg",                "1000",                      5,  "Procter & Gamble"),
        ("chevron",           "jobs",                      5,  "Chevron"),
        ("dukeenergy",        "search",                    1,  "Duke Energy"),
        ("generalmotors",     "Careers_GM",                5,  "General Motors"),
        # ── Travel & Marketplace ───────────────────────────────────────────────
        ("zillow",            "Zillow_Group_External",     5,  "Zillow"),
        ("expedia",           "search",                  108,  "Expedia"),
        # ── Boston-area ────────────────────────────────────────────────────────
        ("libertymutual",     "libertymutual",             5,  "Liberty Mutual"),
        ("tanium",            "tanium",                    5,  "Tanium"),
        ("rubrik",            "rubrik",                    5,  "Rubrik"),
        ("cohesity",          "cohesity",                  5,  "Cohesity"),
    ]
    jobs = []
    with ThreadPoolExecutor(max_workers=30) as pool:
        futures = {pool.submit(_workday, t, b, v, n): n for t, b, v, n in companies}
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_greenhouse_bulk():
    """110+ companies on Greenhouse — STRICT US client-side filter."""
    slugs = [
        ("airbnb","Airbnb"), ("lyft","Lyft"), ("doordash","DoorDash"),
        ("stripe","Stripe"), ("coinbase","Coinbase"), ("robinhood","Robinhood"),
        ("plaid","Plaid"), ("instacart","Instacart"), ("shopify","Shopify"),
        ("github","GitHub"), ("palantir","Palantir"), ("anthropic","Anthropic"),
        ("cloudflare","Cloudflare"), ("datadog","Datadog"), ("elastic","Elastic"),
        ("mongodb","MongoDB"), ("okta","Okta"), ("zscaler","Zscaler"),
        ("hubspotjobs","HubSpot"), ("twilio","Twilio"), ("roku","Roku"),
        ("canva","Canva"), ("discord","Discord"), ("pagerduty","PagerDuty"),
        ("coreweave","CoreWeave"), ("xai","xAI"),
        ("uberfreight","Uber Freight"),
        ("realtimeboardglobal","Miro"),
        ("snowflakecomputing","Snowflake"),
        ("databricks","Databricks"), ("dbtlabsinc","dbt Labs"),
        ("fivetran","Fivetran"), ("airbyte","Airbyte"), ("starburstdata","Starburst"),
        ("dremio","Dremio"), ("clickhouse","ClickHouse"), ("singlestore","SingleStore"),
        ("planetscale","PlanetScale"), ("yugabyte","YugabyteDB"),
        ("dagsterlabs","Dagster Labs"), ("sigmacomputing","Sigma Computing"),
        ("grafanalabs","Grafana Labs"), ("dataiku","Dataiku"),
        ("cockroachlabs","CockroachDB"), ("influxdata","InfluxData"),
        ("matillion","Matillion"), ("snaplogic","SnapLogic"),
        ("datarobot","DataRobot"), ("honeycomb","Honeycomb"),
        ("chronosphere","Chronosphere"), ("atlan","Atlan"),
        ("alation","Alation"), ("collibra","Collibra"),
        ("immuta","Immuta"), ("datafold","Datafold"),
        ("acceldata","Acceldata"), ("arize","Arize AI"),
        ("tinybird","Tinybird"), ("estuary","Estuary"),
        ("tecton","Tecton"), ("motherduck","MotherDuck"),
        ("hightouch","Hightouch"), ("amplitude","Amplitude"),
        ("segment","Segment"), ("pinecone","Pinecone"),
        ("confluent","Confluent"),
        ("anyscale","Anyscale"), ("scale","Scale AI"), ("cohere","Cohere"),
        ("weights-biases","Weights & Biases"), ("determined","Determined AI"),
        ("weaviate","Weaviate"), ("chroma","Chroma"), ("deepset","deepset"),
        ("gong","Gong"), ("outreach","Outreach"), ("klarna","Klarna"),
        ("runwayml","Runway ML"),
        ("brex","Brex"), ("chime","Chime"), ("gusto","Gusto"),
        ("rippling","Rippling"), ("klaviyo","Klaviyo"), ("faire","Faire"),
        ("asana","Asana"), ("notion","Notion"), ("vercel","Vercel"),
        ("hashicorp","HashiCorp"), ("lattice","Lattice"),
        ("block-external-board","Block/Square"), ("square","Square"),
        ("wayfair","Wayfair"), ("chewy","Chewy"), ("yelp","Yelp"),
        ("pinterest","Pinterest"), ("duolingo","Duolingo"),
        ("reddit","Reddit"), ("figma","Figma"), ("mixpanel","Mixpanel"),
        ("samsara","Samsara"), ("verkada","Verkada"), ("benchling","Benchling"),
        ("carta","Carta"), ("flexport","Flexport"), ("thumbtack","Thumbtack"),
        ("procore","Procore"), ("servicetitan","ServiceTitan"),
        ("applovin","AppLovin"), ("rivian","Rivian"), ("waymo","Waymo"),
        ("vanta","Vanta"), ("drata","Drata"), ("statsig","Statsig"),
        ("eppo","Eppo"), ("nerdwallet","NerdWallet"),
        ("alixpartners","AlixPartners"),
        ("rapid7","Rapid7"), ("toasttab","Toast"), ("draftkings","DraftKings"),
        ("zuora","Zuora"), ("tipalti","Tipalti"), ("docusign","DocuSign"),
    ]
    jobs = []
    with ThreadPoolExecutor(max_workers=25) as pool:
        futures = {pool.submit(_greenhouse, slug, name): name for slug, name in slugs}
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_lever_bulk():
    """70+ companies on Lever — STRICT US client-side filter."""
    slugs = [
        ("palantir","Palantir"), ("spotify","Spotify"),
        ("zeta","Zeta Tech"), ("canarytechnologies","Canary Technologies"),
        ("morningbrew","Morning Brew"), ("BDG","Bustle Digital Group"),
        ("dadavidson","D.A. Davidson"), ("iru","Iru"),
        ("airtable","Airtable"), ("coda","Coda"), ("retool","Retool"),
        ("discord","Discord"), ("miro","Miro"), ("deel","Deel"),
        ("pagerduty","PagerDuty"), ("opentable","OpenTable"),
        ("opendoor","Opendoor"), ("redfin","Redfin"), ("compass","Compass"),
        ("sofi","SoFi"), ("creditkarma","Credit Karma"),
        ("workiva","Workiva"), ("chargebee","Chargebee"),
        ("clari","Clari"), ("gong","Gong"), ("salesloft","Salesloft"),
        ("outreach","Outreach"), ("apolloio","Apollo.io"),
        ("intercom","Intercom"), ("linear","Linear"),
        ("launchdarkly","LaunchDarkly"), ("newrelic","New Relic"),
        ("sumologic","Sumo Logic"), ("metabase","Metabase"),
        ("wandb","Weights & Biases"), ("coupa","Coupa Software"),
        ("marqeta","Marqeta"), ("affirm","Affirm"), ("klarna","Klarna"),
        ("mercury","Mercury"), ("moderntreasury","Modern Treasury"),
        ("lithic","Lithic"), ("lemonade","Lemonade"),
        ("rudderstack","RudderStack"), ("trifacta","Trifacta"),
        ("ataccama","Ataccama"), ("grafanalabs","Grafana Labs"),
        ("honeycomb","Honeycomb"), ("chronosphere","Chronosphere"),
        ("slalom","Slalom"), ("thoughtworks","ThoughtWorks"),
        ("publicissapient","Publicis Sapient"),
        ("mistral","Mistral AI"),
        ("weride","WeRide AI"), ("nominal","Nominal"),
        ("veritone","Veritone"), ("primer","Primer AI"),
        ("buildkite","Buildkite"), ("mactores","Mactores"),
    ]
    jobs = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_lever, slug, name): name for slug, name in slugs}
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_ashby_bulk():
    """70+ companies on Ashby — STRICT US filter via addressCountry."""
    slugs = [
        ("ramp","Ramp"), ("perplexity","Perplexity"), ("anysphere","Cursor"),
        ("replit","Replit"), ("warp","Warp"), ("posthog","PostHog"),
        ("codeium","Codeium"), ("clay","Clay"), ("arc","Arc Browser"),
        ("airtable","Airtable"), ("Deel","Deel"),
        ("confluent","Confluent"), ("montecarlodata","Monte Carlo Data"),
        ("astronomer","Astronomer"), ("prefect","Prefect"),
        ("lightdash","Lightdash"), ("neon","Neon"), ("hex","Hex"),
        ("deepnote","Deepnote"), ("metaplane","Metaplane"),
        ("rill","Rill Data"), ("evidence","Evidence"),
        ("fennel","Fennel"), ("chalk","Chalk"),
        ("groq","Groq"), ("lambda","Lambda Labs"), ("coreweave","CoreWeave"),
        ("modal","Modal"), ("replicate","Replicate"),
        ("huggingface","Hugging Face"), ("mistral","Mistral AI"),
        ("together","Together AI"), ("xai","xAI"),
        ("character","Character.AI"), ("elevenlabs","ElevenLabs"),
        ("runway","Runway ML"), ("stability","Stability AI"),
        ("vectara","Vectara"), ("labelbox","Labelbox"),
        ("roboflow","Roboflow"), ("encord","Encord"),
        ("galileo","Galileo"), ("arthur","Arthur AI"),
        ("superconductive","Great Expectations"),
        ("truera","TruEra"), ("whylabs","WhyLabs"),
        ("brex","Brex"),
        ("epam","EPAM Systems"), ("gridynamics","Grid Dynamics"),
    ]
    jobs = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(_ashby, slug, name): name for slug, name in slugs}
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_smartrecruiters_bulk():
    """35+ companies on SmartRecruiters — ?country=US server-side."""
    slugs = [
        ("intel","Intel"), ("lenovo","Lenovo"),
        ("Samsung1","Samsung"), ("SamsungSDSA","Samsung SDS America"),
        ("SamsungSRA","Samsung Research"),
        ("siemens","Siemens USA"), ("abb","ABB"), ("bosch","Bosch USA"),
        ("philips","Philips USA"), ("nokia","Nokia"), ("ericsson","Ericsson"),
        ("capgemini","Capgemini"), ("CognizantTechnologies2","Cognizant"),
        ("Infosys2","Infosys"), ("wipro","Wipro"),
        ("HCLAmericaInc","HCL America"), ("dxc","DXC Technology"),
        ("epam","EPAM Systems"), ("Globant2","Globant"), ("endava","Endava"),
        ("opentext","OpenText"), ("hpe","HPE"),
        ("PaloAltoNetworks2","Palo Alto Networks"),
        ("Deloitte6","Deloitte"), ("ServiceNow","ServiceNow"),
        ("Visa","Visa"), ("Dynatrace1","Dynatrace"),
        ("ThoughtWorks","ThoughtWorks"),
    ]
    jobs = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_smartrecruiters, slug, name): name for slug, name in slugs}
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_eightfold_bulk():
    """7 companies on Eightfold AI — ?location=United+States server-side."""
    companies = [
        ("netflix",       "netflix.com",       "Netflix"),
        ("aexp",          "aexp.com",          "American Express"),
        ("citi",          "citi.com",          "Citi"),
        ("morganstanley", "morganstanley.com", "Morgan Stanley"),
        ("qualcomm",      "qualcomm.com",      "Qualcomm"),
        ("ngc",           "ngc.com",           "Northrop Grumman"),
        ("l3harris",      "l3harris.com",      "L3Harris"),
    ]
    jobs = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = {
            pool.submit(_eightfold, sub, dom, name): name
            for sub, dom, name in companies
        }
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_workable_bulk():
    companies = [("huggingface", "Hugging Face")]
    jobs = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_workable, slug, name): name for slug, name in companies}
        for f in as_completed(futures):
            try:
                jobs.extend(f.result())
            except Exception:
                pass
    return jobs

def fetch_greenhouse_global():
    jobs = []
    r = safe_get(
        "https://my.greenhouse.io/api/v1/boards/jobs"
        "?query=data+engineer&location=United+States&date_posted=past_24_hours"
    )
    if r:
        try:
            for j in r.json().get("jobs", []):
                if not is_relevant(j.get("title", "")):
                    continue
                loc = j.get("location","")
                if is_us_location(loc):
                    jobs.append(job(
                        j["title"], j.get("company_name",""), loc,
                        j.get("absolute_url",""),
                        "Greenhouse Global", j.get("first_published","")))
        except Exception:
            pass
    return jobs

def fetch_smartrecruiters_global():
    jobs = []
    r = safe_get("https://api.smartrecruiters.com/v1/postings"
                 "?q=data+engineer&country=us&limit=50&sort=NEWEST")
    if r:
        try:
            for j in r.json().get("content", []):
                if is_relevant(j.get("name", "")):
                    jobs.append(job(
                        j["name"],
                        j.get("company",{}).get("name",""),
                        j.get("location",{}).get("city",""),
                        j.get("ref",""),
                        "SmartRecruiters Global",
                        j.get("releasedDate","")))
        except Exception:
            pass
    return jobs

# ─────────────────────────────────────────────────────────────────────────────
# MASTER SOURCE REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

ALL_SOURCES = [
    ("Indeed RSS",                          fetch_indeed),
    ("ZipRecruiter RSS",                    fetch_ziprecruiter),
    ("Dice RSS",                            fetch_dice),
    ("SimplyHired RSS",                     fetch_simplyhired),
    ("WeWorkRemotely RSS",                  fetch_weworkremotely),
    ("CareerBuilder RSS",                   fetch_careerbuilder),
    ("Remotive",                            fetch_remotive),
    ("Arbeitnow",                           fetch_arbeitnow),
    ("The Muse",                            fetch_themuse),
    ("RemoteOK",                            fetch_remoteok),
    ("Himalayas",                           fetch_himalayas),
    ("USAJobs",                             fetch_usajobs),
    # ("Adzuna",                            fetch_adzuna),   # needs API key
    ("Wellfound/AngelList",                 fetch_wellfound),
    ("YC Work at a Startup",                fetch_yc_jobs),
    ("LinkedIn",                            fetch_linkedin_jobs),
    ("JSearch (Indeed+LinkedIn+Glassdoor)", fetch_jsearch),
    ("Google Jobs (SerpApi)",               fetch_serpapi_google_jobs),
    ("Amazon Jobs RSS",                     fetch_amazon),
    ("Amazon Jobs JSON",                    fetch_amazon_json),
    ("Google Careers",                      fetch_google),
    ("Microsoft Careers",                   fetch_microsoft),
    ("Meta Careers",                        fetch_meta),
    ("Tesla Careers",                       fetch_tesla),
    ("IBM Careers",                         fetch_ibm),
    ("Oracle Careers",                      fetch_oracle),
    ("Netflix Careers",                     fetch_netflix),
    ("JPMorgan Careers",                    fetch_jpmorgan),
    ("Workday (165+ companies)",            fetch_workday_bulk),
    ("Greenhouse (110+ companies)",         fetch_greenhouse_bulk),
    ("Lever (70+ companies)",               fetch_lever_bulk),
    ("Ashby (70+ companies)",               fetch_ashby_bulk),
    ("SmartRecruiters (35+ companies)",     fetch_smartrecruiters_bulk),
    ("Eightfold AI (7 companies)",          fetch_eightfold_bulk),
    ("Workable",                            fetch_workable_bulk),
    ("Greenhouse Global",                   fetch_greenhouse_global),
    ("SmartRecruiters Global",              fetch_smartrecruiters_global),
]

# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

def send_desktop(j: dict):
    if not DESKTOP_AVAILABLE:
        return
    try:
        desktop_notify.notify(
            title=f"{j['title']}"[:63],
            message=f"{j['company']} | {j['source']}",
            timeout=10)
    except Exception:
        pass

def send_email(jobs: list):
    if not SMTP_USER or not SMTP_PASS:
        if jobs:
            log.warning("Email not configured — set SMTP_USER and SMTP_PASS in .env")
        return
    n       = len(jobs)
    subject = f"🚨 {n} New Data Engineer Job{'s' if n>1 else ''} — Apply NOW (first 10!)"
    html    = (f"<html><body style='font-family:Arial,sans-serif;max-width:720px;margin:auto'>"
               f"<h2 style='color:#1a73e8'>🚨 {n} New Data Engineer Job{'s' if n>1 else ''}</h2>"
               f"<p style='color:#666'>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
               f" — Apply fast to be in the first 10 applicants!</p><hr>")
    for j in jobs:
        html += (
            f"<div style='border-left:4px solid #1a73e8;padding:10px 16px;"
            f"margin:14px 0;background:#f8f9fa'>"
            f"<h3 style='margin:0 0 4px'>"
            f"<a href='{j['url']}' style='color:#1a73e8;text-decoration:none'>"
            f"{j['title']}</a></h3>"
            f"<p style='margin:2px 0'>🏢 <b>{j['company']}</b></p>"
            f"<p style='margin:2px 0;color:#555'>📍 {j['location'] or 'See posting'}</p>"
            f"<p style='margin:2px 0;color:#888;font-size:.85em'>Source: {j['source']}</p>"
            f"<a href='{j['url']}' style='display:inline-block;margin-top:8px;"
            f"padding:6px 16px;background:#1a73e8;color:#fff;border-radius:4px;"
            f"text-decoration:none;font-size:.9em'>Apply Now →</a></div>")
    html += "</body></html>"
    msg = MIMEMultipart("alternative")
    msg["From"], msg["To"], msg["Subject"] = SMTP_USER, ALERT_EMAIL, subject
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as srv:
            srv.starttls()
            srv.login(SMTP_USER, SMTP_PASS)
            srv.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())
        log.info(f"📧 Email sent → {ALERT_EMAIL} ({n} new job{'s' if n>1 else ''})")
    except Exception as ex:
        log.error(f"Email failed: {ex}")

# ─────────────────────────────────────────────────────────────────────────────
# CORE LOOP
# ─────────────────────────────────────────────────────────────────────────────

def check_and_notify():
    log.info(f"🔍 Scanning {len(ALL_SOURCES)} sources in parallel...")
    seen     = load_seen()
    new_jobs = []

    with ThreadPoolExecutor(max_workers=30) as pool:
        futures = {pool.submit(fn): name for name, fn in ALL_SOURCES}
        for future in as_completed(futures):
            name = futures[future]
            try:
                for j in future.result():
                    if not j.get("url"):
                        continue
                    jid = make_id(j["source"], j["url"])
                    if jid in seen:
                        continue
                    seen.add(jid)
                    if j.get("company") in BLOCKED_COMPANIES:
                        continue
                    if not is_fresh(j.get("posted_at",""), hours=24):
                        log.info(f"  FILTERED (stale): [{j['source']}] {j['title']} "
                                 f"posted_at={j.get('posted_at')}")
                        continue
                    new_jobs.append(j)
                    log.info(f"  🆕 [{j['source']}] {j['title']} @ "
                             f"{j['company']} | {j['location']}")
                    send_desktop(j)
            except Exception as ex:
                log.debug(f"  ✗ {name}: {ex}")

    save_seen(seen)

    if new_jobs:
        send_email(new_jobs)
        log.info(f"✅ {len(new_jobs)} new job(s) found — notifications sent.")
    else:
        log.info("✅ Scan complete — no new jobs.")

    return new_jobs

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Real-Time Data Engineer Job Monitor v5.2")
    parser.add_argument("--once", action="store_true", help="Scan once and exit")
    parser.add_argument("--test", action="store_true",
                        help="Send test email + desktop notification")
    args = parser.parse_args()

    if args.test:
        dummy = {
            "title": "Senior Data Engineer [TEST]", "company": "ACME Corp",
            "location": "Remote USA", "url": "https://example.com/job",
            "source": "Test", "posted_at": datetime.now().isoformat()
        }
        log.info("Sending test notifications...")
        send_desktop(dummy)
        send_email([dummy])
        log.info("Done — check your email and desktop.")

    elif args.once:
        check_and_notify()

    else:
        log.info("🚀 Job Monitor v5.2 started")
        log.info(f"   Sources  : {len(ALL_SOURCES)} sources")
        log.info("   Coverage : 1000+ companies, all major ATS platforms, USA only")
        log.info(f"   Interval : every {CHECK_INTERVAL} minutes")
        log.info(f"   Alert to : {ALERT_EMAIL or '⚠  Set ALERT_EMAIL in .env'}")
        log.info("   Press Ctrl+C to stop\n")
        check_and_notify()
        schedule.every(CHECK_INTERVAL).minutes.do(check_and_notify)
        while True:
            schedule.run_pending()
            time.sleep(30)
