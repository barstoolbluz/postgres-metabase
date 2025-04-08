#!/usr/bin/env bash
# PostgreSQL Reconfiguration Script

# load env vars from config if exists
CONFIG_FILE="${FLOX_ENV_CACHE}/postgres.config"
if [[ -f "$CONFIG_FILE" ]]; then
    source "$CONFIG_FILE"
fi

# define default environment variables
export DEFAULT_PGHOSTADDR="${PGHOSTADDR:-127.0.0.1}"
export DEFAULT_PGPORT="${PGPORT:-15432}"
export DEFAULT_PGUSER="${PGUSER:-pguser}"
export DEFAULT_PGPASS="${PGPASS:-pgpass}"
export DEFAULT_PGDATABASE="${PGDATABASE:-postgres}"
export DEFAULT_PGDIR="${PGDIR:-${FLOX_ENV_CACHE}/postgres}"

# enable/disable debugging
export PG_DEBUG="${PG_DEBUG:-false}"

# debug logging function
debug_log() {
    if [[ "$PG_DEBUG" == "true" ]]; then
        echo "$@"
    fi
}

# is / is not first run?
check_first_run() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        return 0 # True, this is the first run
    else
        return 1 # False, not the first run
    fi
}

# save config to $FLOX_ENV_CACHE
save_config() {
    mkdir -p "$(dirname "$CONFIG_FILE")"
    cat > "$CONFIG_FILE" << EOF
# postgresql configuration - Generated on $(date)
export PGHOSTADDR="$PGHOSTADDR"
export PGPORT="$PGPORT"
export PGUSER="$PGUSER"
export PGPASS="$PGPASS"
export PGDATABASE="$PGDATABASE"
export PGDIR="$PGDIR"
export PG_DEBUG="$PG_DEBUG"
EOF
    chmod 644 "$CONFIG_FILE"
}

# customize postgresql config
prompt_for_config() {
    echo ""
    if gum confirm "$(gum style --foreground 240 'Would you like to customize your PostgreSQL configuration?')" --default=false; then
        PGHOSTADDR=$(gum input --placeholder "$DEFAULT_PGHOSTADDR" --value "$DEFAULT_PGHOSTADDR" --prompt "Host Address: ")
        PGPORT=$(gum input --placeholder "$DEFAULT_PGPORT" --value "$DEFAULT_PGPORT" --prompt "Port: ")
        PGUSER=$(gum input --placeholder "$DEFAULT_PGUSER" --value "$DEFAULT_PGUSER" --prompt "Username: ")
        PGPASS=$(gum input --placeholder "$DEFAULT_PGPASS" --value "$DEFAULT_PGPASS" --prompt "Password: " --password)
        PGDATABASE=$(gum input --placeholder "$DEFAULT_PGDATABASE" --value "$DEFAULT_PGDATABASE" --prompt "Database: ")
        
        if gum confirm "Use default directory for PostgreSQL data?" --default=true; then
            PGDIR="$DEFAULT_PGDIR"
        else
            PGDIR=$(gum input --placeholder "$DEFAULT_PGDIR" --value "$DEFAULT_PGDIR" --prompt "PostgreSQL Data Directory: ")
        fi
    else
        # defaults for gum prompts
        PGHOSTADDR="$DEFAULT_PGHOSTADDR"
        PGPORT="$DEFAULT_PGPORT"
        PGUSER="$DEFAULT_PGUSER"
        PGPASS="$DEFAULT_PGPASS"
        PGDATABASE="$DEFAULT_PGDATABASE"
        PGDIR="$DEFAULT_PGDIR"
    fi
    
    # export user-defined variables
    export PGHOSTADDR PGPORT PGUSER PGPASS PGDATABASE PGDIR
    
    # save to postgres.config in $FLOX_ENV_CACHE
    save_config
}

# update dependent vars
update_dependent_vars() {
    # is $PGDIR an absolute path?
    if [[ ! "$PGDIR" = /* ]]; then
        PGDIR="$(pwd)/$PGDIR"
        export PGDIR
    fi
    
    # set dependent vars with absolute paths
    export PGDATA="$PGDIR/data"
    export PGHOST="$PGDIR/run"
    export PGCONFIGFILE="$PGDIR/postgresql.conf"
    export LOG_PATH="$PGHOST/LOG"
    export SESSION_SECRET="$USER-session-secret"
    export DATABASE_URL="postgresql:///$PGDATABASE?host=$PGHOST&port=$PGPORT"
    
    # debug output
    debug_log "Configuration paths:"
    debug_log "  PGDIR: $PGDIR"
    debug_log "  PGDATA: $PGDATA"
    debug_log "  PGHOST: $PGHOST"
}

# init postgresql
initialize_postgres() {
    mkdir -p "$(dirname "$PGDATA")" && chmod 700 "$(dirname "$PGDATA")"
    rm -rf "$PGDATA" && mkdir -p "$PGDATA" && chmod 700 "$PGDATA"
    
    if [[ "$PG_DEBUG" == "true" ]]; then
        initdb "$PGDATA" --locale=C --encoding=UTF8 -A md5 --auth=trust --username "$PGUSER" --pwfile=<(echo "$PGPASS")
    else
        initdb "$PGDATA" --locale=C --encoding=UTF8 -A md5 --auth=trust --username "$PGUSER" --pwfile=<(echo "$PGPASS") > /dev/null 2>&1
    fi
    
    return $?
}

# create config file
create_config_file() {
    echo "listen_addresses = '$PGHOSTADDR'
port = $PGPORT
unix_socket_directories = '$PGHOST'
unix_socket_permissions = 0700" > "$PGDATA/postgresql.conf"
    return 0
}

# start postgresql
start_postgres() {
    # is $PGHOST an absolute path?
    if [[ ! "$PGHOST" = /* ]]; then
        debug_log "Warning: PGHOST is not an absolute path. Using absolute path instead."
        PGHOST="$(pwd)/$PGHOST"
        export PGHOST
    fi
    
    # create debugging dir
    debug_log "Creating PostgreSQL socket directory at: $PGHOST"
    mkdir -p "$PGHOST" 
    
    # enforce permissions
    chmod 700 "$PGHOST"
    
    # was debugging dir created successfully?
    if [[ ! -d "$PGHOST" ]]; then
        echo "Error: Failed to create PostgreSQL socket directory at $PGHOST"
        return 1
    fi
    
    debug_log "Starting PostgreSQL with socket directory: $PGHOST"
    debug_log "Data directory: $PGDATA"
    
    # start postgres with or without debugging
    if [[ "$PG_DEBUG" == "true" ]]; then
        pg_ctl -D "$PGDATA" -w start -o "-c unix_socket_directories=$PGHOST -c listen_addresses=$PGHOSTADDR -p $PGPORT"
    else
        pg_ctl -D "$PGDATA" -w start -o "-c unix_socket_directories=$PGHOST -c listen_addresses=$PGHOSTADDR -p $PGPORT" > /dev/null 2>&1
    fi
    
    return $?
}

# create the database
create_database() {
    psql -lqt | cut -d \| -f 1 | grep -qw $PGDATABASE || createdb > /dev/null 2>&1
    return 0
}

# stop postgresql
stop_postgres() {
    if [[ "$PG_DEBUG" == "true" ]]; then
        pg_ctl -D "$PGDATA" -m fast -w stop
    else
        pg_ctl -D "$PGDATA" -m fast -w stop > /dev/null 2>&1
    fi
    return $?
}

# first-run setup
first_run_setup() {
    display_postgres_config_ui
    update_dependent_vars
}

# display gum-driven wizard
display_postgres_config_ui() {
    clear
    
    # define custom header + colors
    gum style \
        --border rounded \
        --border-foreground 240 \
        --padding "1 2" \
        --margin "1 0" \
        --width 70 \
        "$(gum style --foreground 27 --bold 'PostgreSQL Configuration')
        
$(gum style --foreground 240 'Reconfiguration of your local PostgreSQL database')"
    
    prompt_for_config
}

# setup postgresql
postgres_setup() {
    debug_log "Setting up PostgreSQL..."
    
    # initialize postgres
    debug_log "Initializing PostgreSQL..."
    initialize_postgres || { echo "Failed to initialize PostgreSQL"; return 1; }
    
    if [[ -f "$PGDATA/PG_VERSION" ]]; then
        debug_log "PostgreSQL data directory initialized successfully"
        
        # create postgres config
        debug_log "Creating PostgreSQL configuration file..."
        create_config_file || { echo "Failed to create configuration file"; return 1; }
        
        # stop existing postgres instance(s)
        debug_log "Ensuring PostgreSQL is not running..."
        if [[ "$PG_DEBUG" == "true" ]]; then
            pg_ctl stop -D "$PGDATA" 2>/dev/null || true
        else
            pg_ctl stop -D "$PGDATA" > /dev/null 2>&1 || true
        fi
        
        # start postgres
        debug_log "Starting PostgreSQL..."
        start_postgres || { echo "Failed to start PostgreSQL"; return 1; }
        
        # create database
        debug_log "Creating database..."
        create_database || { echo "Failed to create database"; return 1; }
        
        # shut down postgres
        debug_log "Stopping PostgreSQL..."
        stop_postgres || { echo "Failed to stop PostgreSQL"; return 1; }
        
        debug_log "PostgreSQL setup completed successfully"
    else
        echo "Error: PostgreSQL data directory was not initialized correctly"
        return 1
    fi
    
    return 0
}

# main
main() {
    # removes existing config file to force reconfiguration
    rm -f "$CONFIG_FILE"
    
    # run setup
    first_run_setup
    postgres_setup
    
    echo "PostgreSQL reconfiguration completed successfully."
    echo "You can start PostgreSQL again with 'flox services start postgres'"
}

# runnit
main
