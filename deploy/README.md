# IngredientHub Deployment Guide

This guide covers deploying IngredientHub to a Vultr VPS with nginx, SSL, and CI/CD.

## Architecture

```
                         Vultr VPS
┌────────────────────────────────────────────────────────┐
│                                                        │
│  nginx (80/443)                                        │
│    │                                                   │
│    ├─► ingredients.bodytools.work                      │
│    │     └─► /var/www/ingredienthub/ (static React)    │
│    │                                                   │
│    └─► ingredients.bodytools.work/api/*                │
│          └─► proxy_pass localhost:8001                 │
│                    │                                   │
│                    ▼                                   │
│              FastAPI (systemd service)                 │
│              port 8001                                 │
│                    │                                   │
│                    ▼                                   │
│              Scrapers (subprocess with Xvfb)           │
│                                                        │
│                    │                                   │
│                    ▼                                   │
│              Supabase (remote - no local DB)           │
└────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Initial VPS Setup

SSH into your VPS and run:

```bash
# Download and run setup script
curl -sL https://raw.githubusercontent.com/gregsimek/IngredientHub/main/deploy/setup.sh -o setup.sh
chmod +x setup.sh
sudo ./setup.sh ingredients.bodytools.work https://github.com/gregsimek/IngredientHub.git
```

### 2. Configure Environment

```bash
sudo nano /opt/ingredienthub/backend/.env
```

Fill in:
```env
IO_EMAIL=your_email@example.com
IO_PASSWORD=your_password
SUPABASE_DB_URL=postgresql://postgres.PROJECT_ID:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres
```

### 3. Start Services

```bash
sudo systemctl start ingredienthub-api
sudo systemctl status ingredienthub-api
```

### 4. Set Up GitHub Secrets

In your GitHub repo, go to Settings → Secrets and variables → Actions, and add:

| Secret | Description |
|--------|-------------|
| `VPS_HOST` | Your VPS IP address |
| `VPS_USER` | SSH username (e.g., `ingredienthub` or your user) |
| `VPS_SSH_KEY` | Private SSH key for deployment |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_ANON_KEY` | Supabase anonymous key |

### 5. Generate SSH Key for Deployment

On the VPS:
```bash
ssh-keygen -t ed25519 -C "github-deploy" -f ~/.ssh/github_deploy
cat ~/.ssh/github_deploy.pub >> ~/.ssh/authorized_keys
cat ~/.ssh/github_deploy  # Copy this to VPS_SSH_KEY secret
```

### 6. Push to Deploy

```bash
git push origin main
```

The GitHub Action will automatically:
1. Build the React frontend
2. Deploy backend code
3. Deploy frontend static files
4. Restart the API service

## Manual Deployment

If you need to deploy manually:

```bash
# SSH to VPS
ssh user@your-vps-ip

# Backend
cd /opt/ingredienthub
git pull origin main
cd backend
source venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart ingredienthub-api

# Frontend (build locally first)
# On local machine:
cd frontend
npm run build
rsync -avz dist/ user@your-vps-ip:/var/www/ingredienthub/
```

## File Locations

| Component | Path |
|-----------|------|
| Application code | `/opt/ingredienthub/` |
| Python venv | `/opt/ingredienthub/backend/venv/` |
| Environment file | `/opt/ingredienthub/backend/.env` |
| Frontend files | `/var/www/ingredienthub/` |
| Scraper logs | `/var/log/ingredienthub/` |
| Nginx config | `/etc/nginx/sites-available/ingredienthub` |
| Systemd service | `/etc/systemd/system/ingredienthub-api.service` |
| Xvfb service | `/etc/systemd/system/xvfb.service` |

## Useful Commands

```bash
# Service management
sudo systemctl status ingredienthub-api
sudo systemctl restart ingredienthub-api
sudo systemctl stop ingredienthub-api

# View logs
sudo journalctl -u ingredienthub-api -f                     # API logs
sudo tail -f /var/log/ingredienthub/*.log                   # Scraper logs
sudo tail -f /var/log/nginx/ingredienthub.error.log         # Nginx errors

# Test nginx config
sudo nginx -t

# Reload nginx
sudo systemctl reload nginx

# Check Xvfb
sudo systemctl status xvfb
ps aux | grep Xvfb
```

## SSL Certificate

SSL is set up automatically by certbot during initial setup. To renew or troubleshoot:

```bash
# Check certificate status
sudo certbot certificates

# Renew all certificates
sudo certbot renew

# Force renew
sudo certbot renew --force-renewal

# Manually request certificate
sudo certbot --nginx -d ingredients.bodytools.work
```

## Headed Browser (Playwright)

The IO scraper requires a headed browser because the site detects headless browsers.

On the VPS, this is handled by:
1. **Xvfb** - Virtual framebuffer providing a fake display
2. **xvfb-run** - Wrapper that starts Xvfb for the scraper process

The API automatically wraps scraper commands with `xvfb-run -a` on Linux.

## Troubleshooting

### API won't start
```bash
# Check service status
sudo systemctl status ingredienthub-api

# Check logs
sudo journalctl -u ingredienthub-api -n 50

# Common issues:
# - .env file missing or incorrect
# - Python dependencies not installed
# - Port 8001 already in use
```

### Scraper fails
```bash
# Check scraper logs
ls -la /var/log/ingredienthub/
tail -f /var/log/ingredienthub/IO_scraper_*.log

# Test scraper manually
cd /opt/ingredienthub/backend
source venv/bin/activate
xvfb-run -a python IO_scraper.py --max-products 5
```

### 502 Bad Gateway
```bash
# API not running
sudo systemctl status ingredienthub-api

# Wrong port
grep proxy_pass /etc/nginx/sites-available/ingredienthub
# Should be: proxy_pass http://127.0.0.1:8001/api/;
```

### SSL issues
```bash
# Check certificate
sudo certbot certificates

# Renew
sudo certbot renew --dry-run
```
