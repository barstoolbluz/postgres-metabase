#!/usr/bin/env fish

# reconfigure postgresql
function pgconfigure
    flox services stop postgres
    
    bash -i "$FLOX_ENV_CACHE/helper-functions/pgconfigure.sh"
    
    flox services start postgres
end

# fetch README.md
function readme
    if test "$argv[1]" = "--refresh" -o ! -f "$README_PATH"
        mkdir -p (dirname "$README_PATH") 2>/dev/null
        if command -v curl >/dev/null 2>&1
            curl -s -o "$README_PATH" "$README_URL" >/dev/null 2>&1 || true
        else if command -v wget >/dev/null 2>&1
            wget -q -O "$README_PATH" "$README_URL" >/dev/null 2>&1 || true
        end
    end
    if test -f "$README_PATH"
        bat "$README_PATH" 2>/dev/null || true
    end
end

# fetch fetch.sh
function fetch
    set FETCH_SCRIPT "$RESOURCES_DIR/fetch.sh"
    if test -f "$FETCH_SCRIPT"
        bash -i "$FETCH_SCRIPT" $argv >/dev/null 2>&1 || true
    end
end

# show default help message
function info
    # Determine display host
    set display_host ""
    if test "$PGHOSTADDR" = "0.0.0.0"
        set display_host "localhost"
    else
        set display_host "$PGHOSTADDR"
    end

    # Create the help message with Gum styling
    gum style \
        --border rounded \
        --border-foreground 240 \
        --padding "1 2" \
        --margin "1 0" \
        --width 104 \
        "$(gum style --foreground 141 --bold 'This is a  F l o x  PostgreSQL / Metabase Environment')

👉  Service Management:
    $(gum style --foreground 212 'flox activate -s')                                                        Start PostgreSQL and Metabase at activation
    $(gum style --foreground 212 'flox services <start|stop|restart> <service>')    Start/stop/restart \`postgres\` or \`metabase\`

👉  Configuration:
    $(gum style --foreground 212 'pgconfigure')                                     Reconfigure PostgreSQL post activation

👉  Connect to PostgreSQL:
    $(gum style --foreground 212 'psql')                                            Connect to PostgreSQL

👉  PostgreSQL Connection Details:
    PostgreSQL Host:     $(gum style --foreground 212 "$display_host")
    PostgreSQL Port:     $(gum style --foreground 212 "$PGPORT")
    PostgreSQL Database: $(gum style --foreground 212 "$PGDATABASE")
    PostgreSQL User:     $(gum style --foreground 212 "$PGUSER")

👉  Metabase Connection Details:
    Metabase Host:       $(gum style --foreground 212 "$display_host:3000")"

    echo ""
end
