#!/usr/bin/env bash
set -e

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$APP_DIR/.venv"
PORT="${MINER_PORT:-8920}"

echo "============================================================"
echo "  Twitch & Kick Drops Miner - Linux Installer"
echo "============================================================"

if ! command -v python3 &>/dev/null; then
    echo "[ERROR] python3 is not installed. Install it via apt/dnf/pacman first."
    exit 1
fi

echo "[1/3] Creating Python virtual environment in $VENV_DIR..."
python3 -m venv "$VENV_DIR"

echo "[2/3] Installing Python dependencies..."
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"

mkdir -p "$APP_DIR/data"

echo "[3/3] Creating start script (run.sh)..."
cat > "$APP_DIR/run.sh" <<EOF
#!/usr/bin/env bash
cd "$APP_DIR"
exec "$VENV_DIR/bin/python" main.py "\$@"
EOF
chmod +x "$APP_DIR/run.sh"

echo ""
read -r -p "Would you like to install a systemd user service to run 24/7 in the background? [y/N] " INSTALL_SYSTEMD
if [[ "$INSTALL_SYSTEMD" =~ ^[Yy]$ ]]; then
    SYSTEMD_USER_DIR="$HOME/.config/systemd/user"
    mkdir -p "$SYSTEMD_USER_DIR"
    SERVICE_FILE="$SYSTEMD_USER_DIR/twitch-kick-miner.service"

    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Twitch & Kick Drops Miner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$VENV_DIR/bin/python $APP_DIR/main.py --host 0.0.0.0 --port $PORT
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
EOF

    systemctl --user daemon-reload
    systemctl --user enable --now twitch-kick-miner.service
    echo ""
    echo "[OK] Systemd user service 'twitch-kick-miner.service' installed and started!"
    echo "     Check status : systemctl --user status twitch-kick-miner"
    echo "     View logs    : journalctl --user -u twitch-kick-miner -f"
    echo "     Tip: Run 'sudo loginctl enable-linger $USER' to keep it running after logout."
fi

echo ""
echo "============================================================"
echo "  Installation Complete!"
echo "  Start manually : ./run.sh"
echo "  Web Dashboard  : http://localhost:$PORT"
echo "============================================================"
