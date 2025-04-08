#!/usr/bin/env bash

# reconfigure postgresql at runtime
pgconfigure() {
    flox services stop postgres
    
    bash "${FLOX_ENV_CACHE}/helper-functions/pgconfigure.sh"
    
    flox services start postgres
}

# fetch README.md
readme() {
    if [[ "$1" == "--refresh" ]] || [[ ! -f "$README_PATH" ]]; then
        mkdir -p "$(dirname "$README_PATH")" 2>/dev/null
        if command -v curl >/dev/null 2>&1; then
            curl -s -o "$README_PATH" "$README_URL" >/dev/null 2>&1 || true
        elif command -v wget >/dev/null 2>&1; then
            wget -q -O "$README_PATH" "$README_URL" >/dev/null 2>&1 || true
        fi
    fi
    [[ -f "$README_PATH" ]] && bat "$README_PATH" 2>/dev/null || true
}

# fetch fetch.sh
fetch() {
    local FETCH_SCRIPT="${RESOURCES_DIR}/fetch.sh"
    [[ -f "$FETCH_SCRIPT" ]] && bash "$FETCH_SCRIPT" "$@" >/dev/null 2>&1 || true
}

# show default help message
info() {
    # Determine display host
    local display_host
    if [[ "$PGHOSTADDR" == "0.0.0.0" ]]; then
        display_host="localhost"
    else
        display_host="$PGHOSTADDR"
    fi

    # Create the help message with Gum styling
    gum style \
        --border rounded \
        --border-foreground 240 \
        --padding "1 2" \
        --margin "1 0" \
        --width 104 \
        "$(gum style --foreground 141 --bold 'This is a  F l o x  PostgreSQL / Metabase Environment')

👉  Service Management:
    $(gum style --foreground 212 'flox activate -s')                                Start PostgreSQL and Metabase at activation
    $(gum style --foreground 212 'flox services <start|stop|restart> <service>')    Start/stop/restart \`postgres\` or \`metabase\`

👉  Configuration:
    $(gum style --foreground 212 'pgconfigure')                                     Reconfigure PostgreSQL post activation

👉  Connect to PostgreSQL:
    $(gum style --foreground 212 'psql')                                            Connect to PostgreSQL

👉  PostgreSQL Connection Details:
    PostgreSQL Host:     $(gum style --foreground 212 "${display_host}")
    PostgreSQL Port:     $(gum style --foreground 212 "${PGPORT}")
    PostgreSQL Database: $(gum style --foreground 212 "${PGDATABASE}")
    PostgreSQL User:     $(gum style --foreground 212 "${PGUSER}")

👉  Metabase Connection Details:
    Metabase Host:       $(gum style --foreground 212 "${display_host}:3000")"

    echo ""
}

# export
export -f pgconfigure
export -f readme
export -f fetch
export -f info
