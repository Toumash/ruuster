#!/bin/bash

# Path to the project root
PROJECT_DIR=$(pwd)

echo "Compiling project first..."
cargo build -p apps --bin server --bin ruuster_chat

if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi

echo "Launching terminals..."

# Detect OS and launch terminals accordingly
launch_terminal() {
    local title="$1"
    local cmd="$2"
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS - use osascript with Terminal.app
        osascript -e "tell application \"Terminal\" to do script \"cd $PROJECT_DIR && $cmd\""
    elif command -v gnome-terminal &> /dev/null; then
        # GNOME Terminal (Ubuntu, Fedora with GNOME, etc.)
        gnome-terminal --title="$title" -- bash -c "cd $PROJECT_DIR && $cmd; exec bash"
    elif command -v konsole &> /dev/null; then
        # KDE Konsole
        konsole --new-tab -e bash -c "cd $PROJECT_DIR && $cmd; exec bash" &
    elif command -v xfce4-terminal &> /dev/null; then
        # XFCE Terminal
        xfce4-terminal --title="$title" -e "bash -c 'cd $PROJECT_DIR && $cmd; exec bash'" &
    elif command -v xterm &> /dev/null; then
        # Fallback to xterm
        xterm -T "$title" -e "cd $PROJECT_DIR && $cmd; exec bash" &
    elif command -v alacritty &> /dev/null; then
        # Alacritty
        alacritty --title "$title" -e bash -c "cd $PROJECT_DIR && $cmd; exec bash" &
    elif command -v kitty &> /dev/null; then
        # Kitty
        kitty --title "$title" bash -c "cd $PROJECT_DIR && $cmd; exec bash" &
    else
        echo "Error: No supported terminal emulator found."
        echo "Please install one of: gnome-terminal, konsole, xfce4-terminal, xterm, alacritty, kitty"
        exit 1
    fi
}

# 1. Start Server
launch_terminal "Ruuster Server" "cargo run --bin server"

# Wait a bit for server to start
sleep 2

# 2. Start Client A (Alice)
launch_terminal "Chat - Alice" "echo 'Instruction: Enter server http://127.0.0.1:50051, Room: general' && cargo run --bin ruuster_chat"

# 3. Start Client B (Bob)
launch_terminal "Chat - Bob" "echo 'Instruction: Enter server http://127.0.0.1:50051, Room: general' && cargo run --bin ruuster_chat"

echo "Demo launched! Check your opened Terminal windows."
echo ""
echo "Instructions:"
echo "  1. In each client, press Enter to accept default server address"
echo "  2. Type a room name (e.g., 'general') and press Enter"
echo "  3. Start chatting! Messages will appear in both clients."
echo "  4. Press Ctrl+C or Esc to quit"
