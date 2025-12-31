#!/bin/bash
#
# IngredientHub VPS Initial Setup Script
#
# Usage: sudo ./setup.sh
#
# This script sets up a fresh VPS for IngredientHub deployment.
# Run this once on a new server, then use CI/CD for deployments.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    log_error "Please run as root (sudo ./setup.sh)"
    exit 1
fi

# ============================================================================
# Configuration
# ============================================================================

DOMAIN="${1:-ingredients.bodytools.work}"  # Pass domain as first argument
APP_USER="ingredienthub"
APP_DIR="/opt/ingredienthub"
WEB_DIR="/var/www/ingredienthub"
LOG_DIR="/var/log/ingredienthub"
REPO_URL="${2:-https://github.com/gregsimek/IngredientHub.git}"

log_info "Setting up IngredientHub for domain: $DOMAIN"

# ============================================================================
# System Dependencies
# ============================================================================

log_info "Installing system dependencies..."

apt-get update
apt-get install -y \
    python3.11 \
    python3.11-venv \
    python3.11-dev \
    python3-pip \
    nodejs \
    npm \
    nginx \
    certbot \
    python3-certbot-nginx \
    xvfb \
    git \
    curl \
    wget \
    htop \
    rsync

# ============================================================================
# Create App User
# ============================================================================

if id "$APP_USER" &>/dev/null; then
    log_info "User $APP_USER already exists"
else
    log_info "Creating app user: $APP_USER"
    useradd --system --shell /bin/bash --create-home --home-dir /home/$APP_USER $APP_USER
fi

# ============================================================================
# Create Directories
# ============================================================================

log_info "Creating directories..."

mkdir -p $APP_DIR
mkdir -p $WEB_DIR
mkdir -p $LOG_DIR

chown -R $APP_USER:$APP_USER $APP_DIR
chown -R $APP_USER:$APP_USER $LOG_DIR
chown -R www-data:www-data $WEB_DIR

# ============================================================================
# Clone Repository
# ============================================================================

if [ -d "$APP_DIR/.git" ]; then
    log_info "Repository already cloned, pulling latest..."
    cd $APP_DIR
    sudo -u $APP_USER git fetch origin main
    sudo -u $APP_USER git reset --hard origin/main
else
    log_info "Cloning repository..."
    sudo -u $APP_USER git clone $REPO_URL $APP_DIR
fi

# ============================================================================
# Setup Python Virtual Environment
# ============================================================================

log_info "Setting up Python virtual environment..."

cd $APP_DIR/backend

if [ ! -d "venv" ]; then
    sudo -u $APP_USER python3.11 -m venv venv
fi

sudo -u $APP_USER bash -c "source venv/bin/activate && pip install --upgrade pip"
sudo -u $APP_USER bash -c "source venv/bin/activate && pip install -r requirements.txt"

# ============================================================================
# Install Playwright Browsers
# ============================================================================

log_info "Installing Playwright browsers..."

sudo -u $APP_USER bash -c "source venv/bin/activate && playwright install chromium"
playwright install-deps chromium

# ============================================================================
# Create Environment File
# ============================================================================

if [ ! -f "$APP_DIR/backend/.env" ]; then
    log_warn "Creating placeholder .env file - you need to fill in the values!"
    cat > $APP_DIR/backend/.env << 'EOF'
# IngredientsOnline credentials (if needed)
IO_EMAIL=your_email@example.com
IO_PASSWORD=your_password

# Supabase PostgreSQL connection
SUPABASE_DB_URL=postgresql://postgres.PROJECT_ID:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres
EOF
    chown $APP_USER:$APP_USER $APP_DIR/backend/.env
    chmod 600 $APP_DIR/backend/.env
    log_warn "IMPORTANT: Edit $APP_DIR/backend/.env with your credentials!"
fi

# ============================================================================
# Setup Xvfb Service (for headed browser)
# ============================================================================

log_info "Setting up Xvfb service..."

cat > /etc/systemd/system/xvfb.service << 'EOF'
[Unit]
Description=X Virtual Frame Buffer Service
After=network.target

[Service]
ExecStart=/usr/bin/Xvfb :99 -screen 0 1920x1080x24
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable xvfb
systemctl start xvfb

# ============================================================================
# Setup API Service
# ============================================================================

log_info "Setting up API service..."

cp $APP_DIR/deploy/ingredienthub-api.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable ingredienthub-api

# Don't start yet - need .env to be configured
log_warn "API service installed but not started. Configure .env first, then run:"
log_warn "  sudo systemctl start ingredienthub-api"

# ============================================================================
# Setup Nginx
# ============================================================================

log_info "Setting up Nginx..."

# Update domain in nginx config
sed "s/ingredienthub.yourdomain.com/$DOMAIN/g" $APP_DIR/deploy/nginx.conf > /etc/nginx/sites-available/ingredienthub

# Enable site
ln -sf /etc/nginx/sites-available/ingredienthub /etc/nginx/sites-enabled/

# Test and reload nginx
nginx -t
systemctl reload nginx

# ============================================================================
# SSL Certificate
# ============================================================================

log_info "Requesting SSL certificate..."
log_warn "If this fails, make sure DNS for $DOMAIN points to this server first."

certbot --nginx -d $DOMAIN --non-interactive --agree-tos --email admin@$DOMAIN || {
    log_warn "Certbot failed - you can run it manually later:"
    log_warn "  sudo certbot --nginx -d $DOMAIN"
}

# ============================================================================
# Firewall
# ============================================================================

log_info "Configuring firewall..."

if command -v ufw &> /dev/null; then
    ufw allow 'Nginx Full'
    ufw allow ssh
    log_info "UFW configured for Nginx and SSH"
fi

# ============================================================================
# Create placeholder frontend
# ============================================================================

log_info "Creating placeholder frontend..."

cat > $WEB_DIR/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>IngredientHub</title>
    <style>
        body { font-family: system-ui; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #1a1a2e; color: #eee; }
        .container { text-align: center; }
        h1 { color: #0ea5e9; }
    </style>
</head>
<body>
    <div class="container">
        <h1>IngredientHub</h1>
        <p>Deployment in progress...</p>
        <p>Push to main branch to deploy the React frontend.</p>
    </div>
</body>
</html>
EOF

chown www-data:www-data $WEB_DIR/index.html

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "============================================================================"
echo -e "${GREEN}Setup complete!${NC}"
echo "============================================================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Configure environment variables:"
echo "   sudo nano $APP_DIR/backend/.env"
echo ""
echo "2. Start the API service:"
echo "   sudo systemctl start ingredienthub-api"
echo "   sudo systemctl status ingredienthub-api"
echo ""
echo "3. Set up GitHub Secrets for CI/CD:"
echo "   - VPS_HOST: $(curl -s ifconfig.me)"
echo "   - VPS_USER: $APP_USER (or your SSH user)"
echo "   - VPS_SSH_KEY: Your private SSH key"
echo "   - SUPABASE_URL: Your Supabase project URL"
echo "   - SUPABASE_ANON_KEY: Your Supabase anon key"
echo ""
echo "4. Push to main branch to trigger deployment"
echo ""
echo "5. Access your app at: https://$DOMAIN"
echo ""
echo "Useful commands:"
echo "   sudo systemctl status ingredienthub-api   # Check API status"
echo "   sudo journalctl -u ingredienthub-api -f   # View API logs"
echo "   sudo tail -f /var/log/nginx/ingredienthub.error.log  # Nginx errors"
echo ""
