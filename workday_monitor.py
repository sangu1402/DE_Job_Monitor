"""
Workday Job Monitor - Standalone
426+ Workday companies. Runs in separate CMD from job_monitor_final.py.

USAGE:
  python workday_monitor.py           # Start monitoring
  python workday_monitor.py --once    # Single scan
"""

import os, re, json, time, smtplib, hashlib, logging, argparse, requests, schedule
from datetime import datetime, timezone, timedelta
import zoneinfo
EST = zoneinfo.ZoneInfo("America/New_York")
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

try:
    from plyer import notification as desktop_notify
    DESKTOP_AVAILABLE = True
except ImportError:
    DESKTOP_AVAILABLE = False

load_dotenv()

KEYWORDS = [
    "data engineer", "data engineering", "analytics engineer",
    "dataops engineer", "etl engineer", "pipeline engineer",
    "data platform engineer", "data infrastructure engineer",
]

SEEN_FILE      = "workday_seen_jobs.json"
LOG_FILE       = "workday_monitor.log"
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "2"))

SMTP_HOST   = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER   = os.getenv("SMTP_USER", "")
SMTP_PASS   = os.getenv("SMTP_PASS", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", SMTP_USER)

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

# Dedicated Session for Workday only - not shared with any other source
WD_SESSION = requests.Session()
WD_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

def is_relevant(title):
    return any(kw in title.lower() for kw in KEYWORDS)

def make_id(source, uid):
    return hashlib.md5(f"{source}::{uid}".encode()).hexdigest()

def load_seen():
    try:
        with open(SEEN_FILE) as f: return set(json.load(f))
    except: return set()

def save_seen(seen):
    with open(SEEN_FILE, "w") as f: json.dump(list(seen), f)

def job(title, company, location, url, source, posted=""):
    return {"title": str(title).strip(), "company": str(company).strip(),
            "location": str(location).strip(), "url": str(url).strip(),
            "source": source, "posted_at": posted}

NON_US_LOCATIONS = [
    "mexico","italy","india","canada","uk","germany","france",
    "australia","brazil","spain","poland","ireland","netherlands",
    "singapore","japan","china","korea","toronto","vancouver",
    "montreal","milan","rome","london","berlin","amsterdam",
    "karnataka","bengaluru","bangalore","hyderabad","makati",
    "philippines","chihuahua","ciudad juarez","monza",
    "mumbai","pune","noida","chennai","kolkata",
]

def _workday(tenant, board, wd_ver, company_name):
    jobs = []
    url = f"https://{tenant}.wd{wd_ver}.myworkdayjobs.com/wday/cxs/{tenant}/{board}/jobs"
    for offset in [0, 50]:
        payload = {"appliedFacets": {}, "limit": 50, "offset": offset, "searchText": "data engineer"}
        try:
            r = WD_SESSION.post(url, json=payload, timeout=15)
            if r.status_code != 200:
                log.info(f"  [WD] {company_name}: status {r.status_code}")
                break
        except:
            break
        try:
            data = r.json()
            total = data.get("total", 0)
            if total > 0 and offset == 0:
                log.info(f"  [WD] {company_name}: {total} total jobs")
            for j in data.get("jobPostings", []):
                title = j.get("title","")
                if not is_relevant(title):
                    continue
                title_low = title.lower()
                if any(x in title_low for x in ["_noida","_bangalore","_hyderabad",
                        "_mumbai","_pune","_india"," - india","(india)"]):
                    continue
                if re.match(r'^IN_', title):
                    continue
                locs = j.get("locations",[])
                loc = locs[0].get("type","") if locs else ""
                loc_low = loc.lower()
                if any(x in loc_low for x in NON_US_LOCATIONS):
                    continue
                ext = j.get("externalPath","")
                jurl = f"https://{tenant}.wd{wd_ver}.myworkdayjobs.com/en-US/{board}{ext}" if ext else ""
                posted = j.get("postedOn","")
                jobs.append(job(title, company_name, loc, jurl, f"Workday/{company_name}", posted))
            if offset + 50 >= total:
                break
        except:
            break
    return jobs

WORKDAY_COMPANIES = [
    ("starfish","Careers",501,"Torch Technologies"),
    ("oclc","OCLC_Careers",1,"OCLC"),
    ("ciena","Careers",5,"Ciena"),
    ("takeda","TakedaCareers",5,"Takeda Pharmaceuticals"),
    ("lonza","Lonza",5,"Lonza"),
    ("aristanetworks","Arista_Careers",5,"Arista Networks"),
    ("analogdevices","External",1,"Analog Devices"),
    ("johnsoncontrols","Johnson_Controls",5,"Johnson Controls"),
    ("jabil","JabilCareers",5,"Jabil"),
    ("starbucks","StarbucksExternal",5,"Starbucks"),
    ("diageo","diageo",5,"Diageo"),
    ("valero","ValeroCareers",5,"Valero"),
    ("3m","Search",1,"3M"),
    ("aecom","AECOMJobs",5,"AECOM"),
    ("aig","aig",1,"AIG"),
    ("amd","amd",5,"AMD"),
    ("amnhealthcare","AMNHealthcare",5,"AMN Healthcare"),
    ("att","ATTGeneral",1,"AT&T"),
    ("abbvie","abbvie",5,"AbbVie"),
    ("abbott","abbottcareers",5,"Abbott"),
    ("acadian","Acadian",5,"Acadian Asset Management"),
    ("accenture","AccentureCareers",103,"Accenture"),
    ("acehardware","AceHardware",5,"Ace Hardware"),
    ("activision","Activision",5,"Activision"),
    ("adeccona","AdeccoNA",5,"Adecco North America"),
    ("adobe","external_experienced",5,"Adobe"),
    ("agilent","Agilent_Careers",5,"Agilent"),
    ("alertmedia","AlertMedia",5,"AlertMedia"),
    ("alliantenergy","AlliantEnergyCareers",5,"Alliant Energy"),
    ("allstate","AllstateInsurance",5,"Allstate"),
    ("ally","ally",5,"Ally Financial"),
    ("alteryx","alteryx",5,"Alteryx"),
    ("americanexpress","americanexpress",5,"American Express"),
    ("amerisourcebergen","AmerisourceBergen",5,"AmerisourceBergen"),
    ("amgen","amgen",5,"Amgen"),
    ("apple","apple",5,"Apple"),
    ("aramark","AramarkCareers",5,"Aramark"),
    ("astrazeneca","Careers",3,"AstraZeneca"),
    ("atlassian","External",5,"Atlassian"),
    ("autodesk","Ext",1,"Autodesk"),
    ("axios","Axios",5,"Axios"),
    ("ghr","Lateral-US",1,"Bank of America"),
    ("barclays","search",5,"Barclays"),
    ("barneys","Barneys",5,"Barneys New York"),
    ("baupost","Baupost",5,"Baupost Group"),
    ("bestbuy","BestBuyJobs",1,"Best Buy"),
    ("bigcommerce","BigCommerce",5,"BigCommerce"),
    ("binance","Binance",5,"Binance.US"),
    ("biibhr","external",3,"Biogen"),
    ("blackhills","BlackHillsCareers",5,"Black Hills Energy"),
    ("blackrock","BlackRock_Professional",1,"BlackRock"),
    ("blizzard","Blizzard",5,"Blizzard Entertainment"),
    ("bloomingdales","Bloomingdales",5,"Bloomingdale's"),
    ("blueorigin","External",5,"Blue Origin"),
    ("boeing","EXTERNAL_CAREERS",1,"Boeing"),
    ("bah","BAH_Jobs",1,"Booz Allen Hamilton"),
    ("bmc","ClearwayHealth",1,"Boston Medical Center"),
    ("box","box",5,"Box"),
    ("broadcom","broadcom",5,"Broadcom"),
    ("broadridge","Careers",5,"Broadridge"),
    ("bumble","Bumble",5,"Bumble"),
    ("buzzfeed","BuzzFeed",5,"BuzzFeed"),
    ("cbre","CBRECareers",5,"CBRE"),
    ("cmegroup","CMEGroup",5,"CME Group"),
    ("cvshealth","CVS_Health_Careers",1,"CVS Health"),
    ("capgemini","capgemini",5,"Capgemini"),
    ("capitalone","Capital_One",12,"Capital One"),
    ("card","Search",5,"Cardinal Health"),
    ("care.com","care",5,"Care.com"),
    ("careerbuilder","CareerBuilder",5,"CareerBuilder"),
    ("cashapp","CashApp",5,"Cash App"),
    ("caterpillar","caterpillar",5,"Caterpillar"),
    ("centene","Centene_External",5,"Centene"),
    ("schwab","external",1,"Charles Schwab"),
    ("chevron","jobs",5,"Chevron"),
    ("christies","Christies",5,"Christie's"),
    ("cigna","cignacareers",5,"Cigna"),
    ("cisco","Cisco_Careers",5,"Cisco"),
    ("citi","2",5,"Citigroup"),
    ("cloudera","cloudera",5,"Cloudera"),
    ("cnx","external_global",1,"Concentrix"),
    ("cohesity","cohesity",5,"Cohesity"),
    ("comcast","Comcast_Careers",5,"Comcast"),
    ("commonwealth","Commonwealth",5,"Commonwealth Financial"),
    ("communityhealth","CommunityHealthSystems",5,"Community Health Systems"),
    ("coniferhealth","ConiferHealth",5,"Conifer Health"),
    ("conocophillips","External",1,"ConocoPhillips"),
    ("constantcontact","ConstantContact",5,"Constant Contact"),
    ("constellationbrands","ConstellationBrands",5,"Constellation Brands"),
    ("cornerstone","Cornerstone",5,"Cornerstone OnDemand"),
    ("costco","Costco",5,"Costco"),
    ("crosscountry","CrossCountry",5,"Cross Country Healthcare"),
    ("crowdstrike","crowdstrikecareers",5,"CrowdStrike"),
    ("cummins","CumminsCareers",5,"Cummins"),
    ("dxctechnology","DXCJobs",1,"DXC Technology"),
    ("danaher","DanaherJobs",1,"Danaher"),
    ("dell","dell",1,"Dell Technologies"),
    ("deloitte","deloitte",5,"Deloitte"),
    ("deltek","External",1,"Deltek"),
    ("dignityhealth","DignityHealth",5,"Dignity Health"),
    ("dillards","Dillards",5,"Dillard's"),
    ("discover","discover",5,"Discover"),
    ("disney","disneycareer",5,"Disney"),
    ("doitbest","DoItBest",5,"Do it Best"),
    ("docusign","docusign",1,"DocuSign"),
    ("dollartree","dollartreeus",5,"Dollar Tree"),
    ("draftkings","draftkings",1,"DraftKings"),
    ("dropbox","dropbox",5,"Dropbox"),
    ("dukeenergy","search",1,"Duke Energy"),
    ("dynatrace","dynatrace",5,"Dynatrace"),
    ("ey","Global_Experienced_Careers",3,"EY"),
    ("edwards","EdwardsLifesciences",5,"Edwards Lifesciences"),
    ("ea","EA",5,"Electronic Arts"),
    ("elevancehealth","ANT",1,"Elevance Health"),
    ("lilly","LLY",5,"Eli Lilly"),
    ("emerson","emerson",5,"Emerson Electric"),
    ("encompasshealth","EncompassHealth",5,"Encompass Health"),
    ("entegris","EntegrisCareers",5,"Entegris"),
    ("epicgames","Epic_Games",5,"Epic Games"),
    ("etsy","etsy",5,"Etsy"),
    ("everbridge","Everbridge",5,"Everbridge"),
    ("expedia","search",108,"Expedia"),
    ("expeditors","Expeditors",5,"Expeditors International"),
    ("exxonmobil","ExxonMobil_Careers",5,"ExxonMobil"),
    ("f5","F5",5,"F5 Networks"),
    ("fdic","FDIC",5,"FDIC"),
    ("factset","FactSetCareers",1,"FactSet"),
    ("fedex","FedExCareers",5,"FedEx"),
    ("fmr","targeted",1,"Fidelity"),
    ("fil","001",3,"Fidelity International"),
    ("fifththirdbank","FifthThirdBank",5,"Fifth Third Bank"),
    ("fiserv","EXT",5,"Fiserv"),
    ("ford","FordMotorCompany",5,"Ford"),
    ("fortinet","fortinet",5,"Fortinet"),
    ("ge","ge",5,"GE"),
    ("geaerospace","GE_ExternalSite",5,"GE Aerospace"),
    ("geappliances","GEAppliances",5,"GE Appliances"),
    ("gapinc","GAPINC",1,"Gap Inc"),
    ("gartner","EXT",5,"Gartner"),
    ("gemini","Gemini",5,"Gemini"),
    ("genentech","genentech",5,"Genentech"),
    ("gdit","External_Career_Site",5,"General Dynamics IT"),
    ("generalmotors","Careers_GM",5,"General Motors"),
    ("genius","Genius",5,"Genius"),
    ("gitlab","External",5,"GitLab"),
    ("glassdoor","Glassdoor",5,"Glassdoor"),
    ("guggenheim","Guggenheim_Careers",1,"Guggenheim"),
    ("guidewire","guidewire",5,"Guidewire"),
    ("hca","HCA",5,"HCA Healthcare"),
    ("hdt","HDT",5,"HDT"),
    ("hp","hp",5,"HP"),
    ("halliburton","Halliburton_Careers",1,"Halliburton"),
    ("harbourvest","HVP",5,"HarbourVest"),
    ("healthtrust","HealthTrust",5,"HealthTrust"),
    ("henryschein","HenrySchein",5,"Henry Schein"),
    ("homedepot","CareerDepot",5,"Home Depot"),
    ("homeaway","HomeAway",5,"HomeAway"),
    ("honeywell","honeywell",5,"Honeywell"),
    ("hubspot","hubspot",1,"HubSpot"),
    ("hulu","Hulu",5,"Hulu"),
    ("humana","Humana_External_Career_Site",5,"Humana"),
    ("huron","huroncareers",1,"Huron Consulting"),
    ("itw","External",5,"Illinois Tool Works"),
    ("illumina","illumina",5,"Illumina"),
    ("indeed","Indeed",5,"Indeed"),
    ("informatica","informatica",5,"Informatica"),
    ("intel","External",1,"Intel"),
    ("intercontinental","Intercontinental",5,"Intercontinental Exchange"),
    ("intuit","intuit",5,"Intuit"),
    ("ironmountain","iron-mountain-jobs",5,"Iron Mountain"),
    ("jll","JLLCareers",5,"JLL"),
    ("jj","JJ",5,"Johnson & Johnson"),
    ("juniper","juniper",5,"Juniper Networks"),
    ("kla","kla",5,"KLA"),
    ("kpmg","kpmg",5,"KPMG"),
    ("kaiserpermanente","KaiserPermanente",5,"Kaiser Permanente"),
    ("keybank","KeyBankExternal",5,"KeyBank"),
    ("khoros","Khoros",5,"Khoros"),
    ("kindredhealthcare","KindredHealthcare",5,"Kindred Healthcare"),
    ("kohls","Kohls",5,"Kohl's"),
    ("kraken","Kraken",5,"Kraken"),
    ("kroger","External",1,"Kroger"),
    ("l3harris","l3harris",5,"L3Harris"),
    ("lam","lamresearch",5,"Lam Research"),
    ("leidos","External",5,"Leidos"),
    ("libertymutual","libertymutual",5,"Liberty Mutual"),
    ("lifepoint","LifePointHealth",5,"LifePoint Health"),
    ("lincolnfinancial","LincolnFinancialExternal",5,"Lincoln Financial Group"),
    ("linkedin","LinkedIn",5,"LinkedIn"),
    ("lockheedmartin","LM_Careers",5,"Lockheed Martin"),
    ("lordandtaylor","LordAndTaylor",5,"Lord & Taylor"),
    ("lowes","LWS_External_CS",5,"Lowe's"),
    ("mfs","MFS",5,"MFS Investment Management"),
    ("mksinst","MKSCareersAmericas",1,"MKS Instruments"),
    ("macys","Macys",5,"Macy's"),
    ("magento","Magento",5,"Magento"),
    ("mantech","External",1,"ManTech"),
    ("manpowergroup","ManpowerGroupCareers",5,"ManpowerGroup"),
    ("marriott","Marriott_International",5,"Marriott"),
    ("mars","External",3,"Mars"),
    ("marshalls","Marshalls",5,"Marshalls"),
    ("marvell","marvell",5,"Marvell"),
    ("massmutual","MassMutual",5,"MassMutual"),
    ("mastercard","CorporateCareers",1,"Mastercard"),
    ("mastercard","External",1,"Mastercard"),
    ("match","Match",5,"Match Group"),
    ("maxar","Vantor",1,"Maxar Technologies"),
    ("mckesson","McKesson",5,"McKesson"),
    ("mckinsey","Careers",5,"McKinsey"),
    ("medassets","MedAssets",5,"MedAssets"),
    ("medallia","medallia",5,"Medallia"),
    ("medium","Medium",5,"Medium"),
    ("medline","Medline",5,"Medline"),
    ("medtronic","MedtronicCareers",1,"Medtronic"),
    ("menards","Menards",5,"Menards"),
    ("micron","micron",5,"Micron"),
    ("minitab","Minitab",5,"Minitab"),
    ("modernatx","M_tx",1,"Moderna"),
    ("mdlz","External",3,"Mondelez"),
    ("monster","Monster",5,"Monster Worldwide"),
    ("moodys","MCO",5,"Moody's"),
    ("ms","External",5,"Morgan Stanley"),
    ("nbcuniversal","nbcuniversal",5,"NBCUniversal"),
    ("nxp","careers",3,"NXP Semiconductors"),
    ("nyse","NYSE",5,"NYSE"),
    ("nasdaq","Nasdaq",5,"Nasdaq"),
    ("ngr","NGRCareers",5,"National Grid USA"),
    ("nationwide","NationwideExternal",5,"Nationwide"),
    ("navihealth","NaviHealth",5,"NaviHealth"),
    ("neimanmarcus","NeimanMarcus",5,"Neiman Marcus"),
    ("netapp","netapp",5,"NetApp"),
    ("nike","nke",1,"Nike"),
    ("nordstrom","Nordstrom",5,"Nordstrom"),
    ("ntrs","northerntrust",1,"Northern Trust"),
    ("ngc","Northrop_Grumman_External_Site",1,"Northrop Grumman"),
    ("nuro","Nuro",5,"Nuro"),
    ("nutanix","nutanix",5,"Nutanix"),
    ("nvidia","NVIDIAExternalCareerSite",5,"Nvidia"),
    ("okcupid","OKCupid",5,"OKCupid"),
    ("offerup","OfferUp",5,"OfferUp"),
    ("okta","okta",1,"Okta"),
    ("mmc","MMC",1,"Oliver Wyman/MMC"),
    ("orgill","Orgill",5,"Orgill"),
    ("owensminor","OwensMinor",5,"Owens & Minor"),
    ("paccar","PACCAR",5,"PACCAR"),
    ("pimco","PIMCOExternal",5,"PIMCO"),
    ("pnc","External",5,"PNC Bank"),
    ("pseg","PSEGExternal",5,"PSEG"),
    ("pge","PGEExternal",5,"Pacific Gas and Electric Company"),
    ("pacificlife","PacificLife",5,"Pacific Life"),
    ("palomarhealth","PalomarHealth",5,"Palomar Health"),
    ("parallon","Parallon",5,"Parallon"),
    ("parker","ParkerCareers",5,"Parker Hannifin"),
    ("patterson","Patterson",5,"Patterson Companies"),
    ("paypal","jobs",1,"PayPal"),
    ("paychex","PaychexExternal",5,"Paychex"),
    ("pepsico","PepsiCoJobs",5,"PepsiCo"),
    ("pfizer","PfizerCareers",1,"Pfizer"),
    ("phreesia","Phreesia",5,"Phreesia"),
    ("pioneer","Pioneer",5,"Pioneer Investments"),
    ("plentyoffish","PlentyOfFish",5,"Plenty of Fish"),
    ("politico","Politico",5,"Politico"),
    ("premier","Premier",5,"Premier"),
    ("pressganey","Careers",1,"Press Ganey"),
    ("pg","1000",5,"Procter & Gamble"),
    ("progressive","Progressive",5,"Progressive Insurance"),
    ("providence","Providence",5,"Providence Health"),
    ("pru","Careers",5,"Prudential"),
    ("purestorage","purestorage",5,"Pure Storage"),
    ("putnam","Putnam",5,"Putnam Investments"),
    ("pwc","Global_Experienced_Careers",3,"PwC"),
    ("qualcomm","qualcomm",5,"Qualcomm"),
    ("globalhr","REC_RTX_Ext_Gateway",5,"RTX/Raytheon"),
    ("redfin","Redfin",5,"Redfin"),
    ("regeneron","Careers",1,"Regeneron"),
    ("regions","RegionsBank",5,"Regions Financial"),
    ("retailmenot","RetailMeNot",5,"RetailMeNot"),
    ("ringcentral","ringcentral",5,"RingCentral"),
    ("roberthalf","RobertHalfCareers",5,"Robert Half"),
    ("roblox","roblox",5,"Roblox"),
    ("rockwellautomation","External_Rockwell_Automation",1,"Rockwell Automation"),
    ("rubrik","rubrik",5,"Rubrik"),
    ("spgi","SPGI_Careers",5,"S&P Global"),
    ("saic","saic",5,"SAIC"),
    ("concur","Concur",5,"SAP Concur"),
    ("slb","SLBExternal",5,"SLB (Schlumberger)"),
    ("saks","Saks",5,"Saks Fifth Avenue"),
    ("salesforce","External_Career_Site",12,"Salesforce"),
    ("sandia","SandiaExternalCareers",5,"Sandia National Laboratories"),
    ("scripps","Scripps",5,"Scripps Health"),
    ("scrippshealth","ScrippsHealth",5,"Scripps Health"),
    ("selectmedical","SelectMedical",5,"Select Medical"),
    ("sentinelone","sentinelone",5,"SentinelOne"),
    ("servicenow","servicenow",5,"ServiceNow"),
    ("sharphealth","SharpHealth",5,"Sharp HealthCare"),
    ("sharphealthcare","SharpHealthcare",5,"Sharp Healthcare"),
    ("shazam","Shazam",5,"Shazam"),
    ("simplyhired","SimplyHired",5,"SimplyHired"),
    ("slack","External",5,"Slack"),
    ("snap","snap",5,"Snap"),
    ("socialsolutions","SocialSolutions",5,"Social Solutions"),
    ("solarwinds","solarwinds",5,"SolarWinds"),
    ("sonyglobal","SonyJapanCareers",1,"Sony"),
    ("sonymusic","SonyMusic",5,"Sony Music Entertainment"),
    ("sothebys","Sothebys",5,"Sotheby's"),
    ("soundcloud","SoundCloud",5,"SoundCloud"),
    ("spotify","Spotify",5,"Spotify"),
    ("spredfast","Spredfast",5,"Spredfast"),
    ("spreetail","Spreetail",5,"Spreetail"),
    ("sprinklr","Careers",1,"Sprinklr"),
    ("squarespace","squarespace",5,"Squarespace"),
    ("statestreet","Global",1,"State Street"),
    ("stryker","StrykerCareers",1,"Stryker"),
    ("substack","Substack",5,"Substack"),
    ("sutterhealth","SutterHealth",5,"Sutter Health"),
    ("synopsys","synopsys",5,"Synopsys"),
    ("tmobile","External",1,"T-Mobile"),
    ("troweprice","TRowePrice",5,"T. Rowe Price"),
    ("tjx","TJX_EXTERNAL",1,"TJX Companies"),
    ("tableau","Tableau",5,"Tableau"),
    ("tanium","tanium",5,"Tanium"),
    ("target","targetcareers",5,"Target"),
    ("telesign","TeleSign",5,"TeleSign"),
    ("tenethealth","TenetHealth",5,"Tenet Healthcare"),
    ("teradata","teradata",5,"Teradata"),
    ("teradyne","teradyne",5,"Teradyne"),
    ("theathletic","TheAthletic",5,"The Athletic"),
    ("thetradedesk","External",1,"The Trade Desk"),
    ("thermofisher","ThermoFisherCareers",5,"Thermo Fisher"),
    ("thomsonreuters","External_Career_Site",5,"Thomson Reuters"),
    ("thousandeyes","ThousandEyes",5,"ThousandEyes"),
    ("tinder","Tinder",5,"Tinder"),
    ("travelers","External",5,"Travelers"),
    ("trimble","TrimbleCareers",1,"Trimble"),
    ("trinityhealth","TrinityHealth",5,"Trinity Health"),
    ("truevalue","TrueValue",5,"True Value"),
    ("twitter","Twitter",5,"Twitter/X"),
    ("usbank","US_Bank_Careers",1,"U.S. Bank"),
    ("usaa","USAACareers",5,"USAA"),
    ("underarmour","underarmour",5,"Under Armour"),
    ("unify","Careers",1,"Unify Technologies"),
    ("unitedhealthgroup","unitedhealthgroup",5,"UnitedHealth Group"),
    ("unity3d","unity3d",5,"Unity Technologies"),
    ("universalhealth","UniversalHealthServices",5,"Universal Health Services"),
    ("universalmusic","UniversalMusic",5,"Universal Music Group"),
    ("uplight","Uplight",5,"Uplight"),
    ("vmware","vmware",1,"VMware/Broadcom"),
    ("vrbo","VRBO",5,"VRBO"),
    ("vanguard","vanguard_external",5,"Vanguard"),
    ("veeva","Veeva",5,"Veeva Systems"),
    ("verizon","verizon-careers",12,"Verizon"),
    ("vice","Vice",5,"Vice Media"),
    ("vimeo","Vimeo",5,"Vimeo"),
    ("visa","Visa_Careers",5,"Visa"),
    ("vistra","VistraCareers",5,"Vistra Energy"),
    ("vizient","Vizient",5,"Vizient"),
    ("volusion","Volusion",5,"Volusion"),
    ("vox","Vox",5,"Vox Media"),
    ("wpengine","WP Engine",5,"WP Engine"),
    ("walmart","WalmartExternal",5,"Walmart"),
    ("warnerbros","global",5,"Warner Bros Discovery"),
    ("warnermusic","WarnerMusic",5,"Warner Music Group"),
    ("wayfair","wayfair",5,"Wayfair"),
    ("wellington","eFC",5,"Wellington Management"),
    ("wellsfargo","WellsFargoJobs",1,"Wells Fargo"),
    ("westernasset","WesternAsset",5,"Western Asset"),
    ("weyerhaeuser","Weyerhaeuser",5,"Weyerhaeuser"),
    ("workday","Workday",5,"Workday"),
    ("xcelenergy","XcelEnergyCareers",5,"Xcel Energy"),
    ("yahoo","yahooinc",5,"Yahoo"),
    ("zillow","Zillow_Group_External",5,"Zillow"),
    ("ziprecruiter","ZipRecruiter",5,"ZipRecruiter"),
    ("zipcar","Zipcar",5,"Zipcar"),
    ("zoom","zoom",5,"Zoom"),
    ("zoosk","Zoosk",5,"Zoosk"),
    ("athenahealth","athenahealth",5,"athenahealth"),
    ("data.world","DataWorld",5,"data.world"),
    ("ebay","ebay",5,"eBay"),
    ("irhythm","iRhythm",5,"iRhythm Technologies"),
    ("adp","ADP_Careers",5,"ADP"),
    ("aon","Aon_Careers",5,"Aon"),
    ("ansys","ANSYS_Careers",5,"Ansys"),
    ("autozone","AutoZone_Careers",5,"AutoZone"),
    ("bakerhughes","Career",5,"Baker Hughes"),
    ("baxterhr","Baxter_Careers",1,"Baxter International"),
    ("bms","bms",5,"Bristol Myers Squibb"),
    ("bny","BNY_Careers",1,"BNY Mellon"),
    ("blackstone","Blackstone_Careers",1,"Blackstone"),
    ("cadence","Cadence_Careers",5,"Cadence Design Systems"),
    ("chubb","chubb",5,"Chubb"),
    ("cocacola","CocaCola",5,"Coca-Cola"),
    ("conduent","External",5,"Conduent"),
    ("costar","CoStarGroup",5,"CoStar Group"),
    ("dayforce","Dayforce_Careers",5,"Dayforce"),
    ("deutschebank","External",5,"Deutsche Bank"),
    ("equifax","Equifax",5,"Equifax"),
    ("franklintempletonprod","FT_External",1,"Franklin Templeton"),
    ("gallup","Gallup",5,"Gallup"),
    ("genpact","genpact",5,"Genpact"),
    ("hilton","Hilton_Careers",5,"Hilton"),
    ("hsbc","HSBC_Careers",5,"HSBC"),
    ("keysight","Keysight_External",1,"Keysight Technologies"),
    ("merck","SearchJobs",5,"Merck"),
    ("morningstar","morningstar",5,"Morningstar"),
    ("novartis","Novartis_Careers",5,"Novartis"),
    ("redhat","External",1,"Red Hat"),
    ("schneiderelectric","SchneiderElectricCareers",5,"Schneider Electric"),
    ("splunk","Splunk_Careers",5,"Splunk"),
    ("transunion","TransUnion",5,"TransUnion"),
    ("westernunion","External",5,"Western Union"),
    ("spgi","SPGI_Careers",5,"S&P Global"),
    ("terex","Terex",5,"Terex Corporation"),
    ("symbotic","Symbotic",5,"Symbotic"),
    ("tylertech","TylerTechnologies",5,"Tyler Technologies"),
    ("dexcom","Dexcom",5,"Dexcom"),
    ("garmin","GarminJobsExternal",5,"Garmin"),
    ("insulet","InsuletCorporation",5,"Insulet Corporation"),
    ("petsmart","PetSmartCareers",5,"PetSmart"),
    ("resideo","ResideoCareers",5,"Resideo"),
    ("viasat","ViasatCareers",5,"Viasat"),
    ("teladochealth","TeladocHealth",5,"Teladoc Health"),
    ("checkout","Checkout",5,"Checkout.com"),
    ("relx","RELX",5,"RELX"),
    ("hhmi","HHMI",5,"Howard Hughes Medical Institute"),
    ("zendesk","Zendesk",5,"Zendesk"),
    ("carmax","External",1,"CarMax"),
]

def send_desktop(j):
    if not DESKTOP_AVAILABLE: return
    try:
        desktop_notify.notify(title=j['title'][:63], message=f"{j['company']} | {j['source']}", timeout=10)
    except: pass

def send_email(jobs):
    if not SMTP_USER or not SMTP_PASS: return
    n = len(jobs)
    subject = f'Workday Alert: {n} New DE Jobs'
    html = f"<html><body style='font-family:Arial;max-width:720px;margin:auto'>"
    html += f"<h2 style='color:#e65100'>{n} New Workday DE Job{'s' if n>1 else ''}</h2>"
    html += f"<p style='color:#666'>{datetime.now(EST).strftime('%Y-%m-%d %H:%M:%S EST')}</p><hr>"
    for j in jobs:
        html += f"<div style='border-left:4px solid #e65100;padding:10px 16px;margin:14px 0;background:#fff3e0'>"
        html += f"<h3 style='margin:0 0 4px'><a href='{j['url']}' style='color:#e65100'>{j['title']}</a></h3>"
        html += f"<p style='margin:2px 0'>Company: <b>{j['company']}</b></p>"
        html += f"<p style='margin:2px 0;color:#555'>Location: {j['location'] or 'See posting'}</p>"
        html += f"<p style='margin:2px 0;color:#888;font-size:.85em'>Source: {j['source']}</p>"
        html += f"<a href='{j['url']}' style='display:inline-block;margin-top:8px;padding:6px 16px;background:#e65100;color:#fff;border-radius:4px;text-decoration:none'>Apply Now</a></div>"
    html += "</body></html>"
    msg = MIMEMultipart("alternative")
    msg["From"], msg["To"], msg["Subject"] = SMTP_USER, ALERT_EMAIL, subject
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as srv:
            srv.starttls()
            srv.login(SMTP_USER, SMTP_PASS)
            srv.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())
        log.info(f'Email sent -> {ALERT_EMAIL} ({n} Workday jobs)')
    except Exception as ex:
        log.error(f'Email failed: {ex}')

def check_workday():
    log.info(f'Scanning {len(WORKDAY_COMPANIES)} Workday companies...')
    seen = load_seen()
    new_jobs = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_workday, t, b, v, n): n for t, b, v, n in WORKDAY_COMPANIES}
        for f in as_completed(futures):
            try:
                for j in f.result():
                    if not j.get("url"): continue
                    jid = make_id(j["source"], j["url"])
                    if jid in seen: continue
                    seen.add(jid)
                    new_jobs.append(j)
                    log.info(f"  NEW [{j['source']}] {j['title']} @ {j['company']} | {j['location']}")
                    send_desktop(j)
            except Exception as ex:
                log.debug(f'Error: {ex}')
    save_seen(seen)
    if new_jobs:
        send_email(new_jobs)
        log.info(f'{len(new_jobs)} Workday job(s) found.')
    else:
        log.info('Scan complete - no new Workday jobs.')
    return new_jobs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Workday Job Monitor")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.once:
        check_workday()
    else:
        log.info(f'Workday Monitor started ({len(WORKDAY_COMPANIES)} companies, every {CHECK_INTERVAL} min)')
        log.info(f'Alert to: {ALERT_EMAIL}')
        check_workday()
        schedule.every(CHECK_INTERVAL).minutes.do(check_workday)
        while True:
            schedule.run_pending()
            time.sleep(30)
