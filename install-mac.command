#!/bin/bash
# Swiss Caselaw - macOS Installer
# Double-click this file to install

set -e

echo "========================================"
echo "  Swiss Caselaw - Local Search"
echo "  Installing..."
echo "========================================"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check for Python 3
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required but not installed."
    echo ""
    echo "Please install Python from: https://www.python.org/downloads/"
    echo "Or with Homebrew: brew install python3"
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Found Python $PYTHON_VERSION"

# Create virtual environment
echo ""
echo "Setting up environment..."
cd local_app

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
pip install --quiet -e .

# Download database
echo ""
echo "Downloading database (this may take a while on first run)..."
python -m caselaw_local.cli update

# Create launcher script
LAUNCHER="$SCRIPT_DIR/Swiss Caselaw.command"
cat > "$LAUNCHER" << 'LAUNCHER_EOF'
#!/bin/bash
cd "$(dirname "$0")/local_app"
source .venv/bin/activate
echo "Starting Swiss Caselaw..."
echo "Opening browser to http://127.0.0.1:8787"
echo ""
echo "Press Ctrl+C to stop the server."
echo ""
# Open browser after short delay
(sleep 2 && open "http://127.0.0.1:8787") &
python -m caselaw_local.cli serve
LAUNCHER_EOF
chmod +x "$LAUNCHER"

# Create Desktop alias
DESKTOP="$HOME/Desktop"
if [ -d "$DESKTOP" ]; then
    ln -sf "$LAUNCHER" "$DESKTOP/Swiss Caselaw.command"
    echo ""
    echo "Created shortcut on Desktop!"
fi

echo ""
echo "========================================"
echo "  Installation complete!"
echo "========================================"
echo ""
echo "To start Swiss Caselaw:"
echo "  - Double-click 'Swiss Caselaw.command' on your Desktop"
echo "  - Or run: $LAUNCHER"
echo ""
echo "Starting now..."
echo ""

# Start the app
"$LAUNCHER"
