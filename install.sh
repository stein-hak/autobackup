#!/bin/bash
# ZFS Autobackup - Systemd Installation Script

set -e

echo "========================================="
echo "  ZFS Autobackup - Systemd Installation"
echo "========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: Please run as root (use sudo)"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
echo "✓ Python version: $PYTHON_VERSION"

# Check if config exists
if [ ! -f "backup_config.yaml" ]; then
    echo ""
    echo "⚠️  No backup_config.yaml found"
    if [ -f "backup_config.yaml.example" ]; then
        echo "   Copying example config..."
        cp backup_config.yaml.example backup_config.yaml
        echo "   ✓ Created backup_config.yaml from example"
        echo ""
        echo "   IMPORTANT: Edit backup_config.yaml before continuing!"
        echo "   Run: nano backup_config.yaml"
        echo ""
        read -p "Press Enter when config is ready, or Ctrl+C to exit..."
    else
        echo "   ERROR: backup_config.yaml.example not found"
        exit 1
    fi
fi

# Validate config
echo ""
echo "Validating configuration..."
if python3 test-config.py backup_config.yaml; then
    echo "✓ Configuration is valid"
else
    echo "✗ Configuration validation failed"
    echo "  Please fix errors in backup_config.yaml"
    exit 1
fi

# Install Python dependencies (just PyYAML and requests)
echo ""
echo "Installing Python dependencies..."
pip3 install -r requirements.txt --quiet
echo "✓ Dependencies installed (PyYAML, requests)"

# Install systemd service
echo ""
echo "Installing systemd service..."

# Copy service file
cp autobackup.service /etc/systemd/system/
echo "✓ Service file copied to /etc/systemd/system/"

# Reload systemd
systemctl daemon-reload
echo "✓ Systemd daemon reloaded"

# Enable service
systemctl enable autobackup.service
echo "✓ Service enabled (will start on boot)"

# Start service
systemctl start autobackup.service
echo "✓ Service started"

echo ""
echo "========================================="
echo "  Installation Complete!"
echo "========================================="
echo ""
echo "Service status:"
systemctl status autobackup.service --no-pager -l
echo ""
echo "Useful commands:"
echo "  View logs:    sudo journalctl -u autobackup -f"
echo "  Stop service: sudo systemctl stop autobackup"
echo "  Start service: sudo systemctl start autobackup"
echo "  Restart:      sudo systemctl restart autobackup"
echo "  Status:       sudo systemctl status autobackup"
echo "  Disable:      sudo systemctl disable autobackup"
echo ""
