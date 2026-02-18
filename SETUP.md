# Data Engineer Job Monitor v3 — Setup Guide

## Quick Start

```bash
pip install requests feedparser plyer schedule python-dotenv
```

Create `.env`:
```env
SMTP_USER=your@gmail.com
SMTP_PASS=xxxx-xxxx-xxxx-xxxx    # Gmail App Password (not your real password)
ALERT_EMAIL=notify@email.com
ADZUNA_APP_ID=optional           # Free key at developer.adzuna.com adds 1 more source
ADZUNA_APP_KEY=optional
CHECK_INTERVAL=2                 # Minutes between scans
```

> **Gmail App Password**: myaccount.google.com → Security → 2-Step Verification → App Passwords

```bash
python job_monitor.py           # Start monitoring
python job_monitor.py --test    # Test email + desktop notification
python job_monitor.py --once    # Single scan and exit
```

---

## Coverage Summary

| Platform | Companies Covered |
|---|---|
| **Workday** | Apple, Salesforce, Adobe, Nvidia, Cisco, PayPal, ServiceNow, Okta, Zoom, DocuSign, VMware, Dell, HP, Qualcomm, AMD, Intuit, eBay, Snap, Twitter/X, Target, Walmart, Home Depot, Capital One, JPMorgan, Bank of America, Visa, Mastercard, BlackRock, Deloitte, PwC, Accenture, UnitedHealth, CVS, J&J, Pfizer, Boeing, Lockheed, Nike, Disney, AT&T, Verizon, and 80+ more |
| **Greenhouse** | Airbnb, Stripe, Databricks, Snowflake, Confluent, dbt Labs, Fivetran, Coinbase, Robinhood, Plaid, Gusto, Rippling, Shopify, DoorDash, Lyft, Cloudflare, Datadog, MongoDB, Elastic, Palantir, GitHub, Anthropic, and 60+ more |
| **Lever** | Netflix, Reddit, Figma, Canva, Airtable, Retool, Discord, Duolingo, Miro, Brex, Deel, PagerDuty, Atlassian, Gong, Salesoft, and 35+ more |
| **Ashby** | Ramp, Perplexity, Cursor, Replit, Warp, PostHog, Codeium, Together AI, and 30+ more |
| **SmartRecruiters** | Intel, Lenovo, Siemens, Capgemini, Cognizant, Infosys, Wipro, and 15+ more |
| **Direct APIs** | Amazon (RSS), Google, Microsoft, Meta, IBM, Oracle |
| **Job Boards** | Indeed, ZipRecruiter, Dice, SimplyHired, CareerBuilder, WeWorkRemotely, Remotive, Arbeitnow, The Muse, RemoteOK, Himalayas, USAJobs, Adzuna, LinkedIn, Wellfound, YC Startup Jobs |

**Total: 500+ companies, 20+ job boards/APIs**

---

## Run 24/7 for Free (GitHub Actions)

1. Create a GitHub repo, add `job_monitor.py`
2. Place `github_workflow.yml` in `.github/workflows/`
3. Go to repo **Settings → Secrets → Actions** and add:
   - `SMTP_USER`, `SMTP_PASS`, `ALERT_EMAIL`
4. Push — runs every 5 minutes automatically, forever, free

---

## Run as Background Service

```bash
# macOS/Linux — keeps running after terminal closes
nohup python job_monitor.py &> monitor.log &

# Docker
docker run -d --env-file .env -v $(pwd):/app python:3.11 \
  sh -c "pip install requests feedparser plyer schedule python-dotenv && python /app/job_monitor.py"
```

---

## How Notifications Work

- **Desktop popup**: Instant OS notification for each new job found
- **Email**: Sent immediately when new jobs are detected (never sent if nothing new)
- Emails include a direct **Apply Now** button link to the job posting
