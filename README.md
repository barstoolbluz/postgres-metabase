# 🐘 A Flox Environment for PostgreSQL with Metabase

This Flox environment provides wizard-driven setup for PostgreSQL database development with integrated Metabase analytics. The environment automates configuration, service management, and provides helper utilities.

## ✨ Features

- Wizard-driven configuration of PostgreSQL with the option to use built-in defaults
- Built-in Metabase integration for data visualization and analytics
- Service management for both PostgreSQL and Metabase
- Sample data loading with Iowa liquor sales dataset
- Cross-platform compatibility (macOS, Linux)

## 🧰 Included Tools

The environment packs these essential tools:

- `postgresql` - Powerful open-source relational database (PostgreSQL 16 by default)
- `metabase` - Open-source business intelligence and analytics platform
- `gum` - Terminal UI toolkit powering the setup wizard and styling
- `coreutils` - For macOS/Darwin compatibility
- `bash` - For cross-shell and cross-platform compatibility
- `curl` - Used to fetch this `README.md`, helper-function scripts, and database loading scripts.
- `bat` - Used to show this `README.md`
  
## 🏁 Getting Started

### 📋 Prerequisites

- [Flox](https://flox.dev/get) installed on your system

### 💻 Installation & Activation

Jump in with:

1. Clone this repo

```sh
git clone https://github.com/barstoolbluz/postgres-metabase && cd postgres-metabase
```

OR

2. Get the latest version of this environment from FloxHub

```sh
flox activate -r barstoolbluz/postgres-metabase
```

THEN

3. Run:

```sh
flox activate -s
```

Either method:
- Pulls in all dependencies
- Detects any existing PostgreSQL configuration
- Fires up the configuration wizard if needed
- Starts the `postgres` and `metabase` services 

### 🧙 Setup Wizard

First-time activation triggers a wizard that:

1. Looks for existing PostgreSQL configuration in `$FLOX_ENV_CACHE/postgres.config`
2. Kicks off a wizard you can use to customize PostgreSQL if no config is found
3. Saves this config for future use

> **Tip:** Type `readme` at any time to view this README.md in your terminal or IDE.

## 📝 Usage

### 🗄️ Setting Up the Iowa Liquor Sales Database

To create and work with the `iowa_liquor_sales` database:

1. When the setup wizard appears during first-time activation, simply accept all the default options by selecting "No" when asked to customize your PostgreSQL configuration. This will automatically set up your database with the correct name.

2. Once PostgreSQL is running, follow these steps to load the sample data:

   - Run `fetch` to download the Iowa liquor sales dataset (note: this is a 7.1 GB file)
   - Run `populate` to load data from the CSV into your PostgreSQL database
   - The `populate` script includes built-in unit tests that run automatically to confirm the data loaded correctly

### 🛠️ Available Commands

```bash
# Connect to PostgreSQL
psql

# Reconfigure PostgreSQL
pgconfigure

# Service Management
flox services start postgres     # Start PostgreSQL
flox services stop postgres      # Stop PostgreSQL
flox services restart postgres   # Restart PostgreSQL
flox services restart metabase   # Restart Metabase

# Built-in Functions
readme                           # Shows this README.md using bat
info                             # Shows the welcome message
fetch                            # Fetches the Iowa liquor sales dataset
populate                         # Populates the PostgreSQL database with this dataset
pgconfigure                      # Reconfigure PostgreSQL database as needed
```

## 🔍 How It Works

### 🔄 Configuration Management

Our environment implements a multi-tiered config strategy:

1. **Existing Environment Variables**: Uses PostgreSQL environment variables if available
2. **PostgreSQL Config File**: Reads from `postgres.config` if present
3. **Interactive Configuration**: Prompts for configuration details if no valid config is found

We store config files in the following path:
- The directory specified by `DEFAULT_PGDIR` environment variable (if set)
- The directory specified by `PGDIR` environment variable (if set)
- The environment's cache (`$FLOX_ENV_CACHE`); this is the default

### 🐚 Shell Integration

Our environment includes shell integration for multiple shells:
- Bash
- Zsh
- Fish

### 📊 Database & Analytics Integration

Our environment provides:
- PostgreSQL database with your choice of version (10-16)
- PostGIS extension for geographical data
- Metabase for data visualization and analytics:
  - Runs on localhost:3000 by default
  - Automatically configures for connections to your PostgreSQL instance

### 📂 Sample Data Loading

Our environment includes scripts to fetch and load sample data:

1. **Iowa Liquor Sales Dataset**:
   - Use the `fetch` command to download the Iowa liquor sales dataset
   - The file is saved to `$HOME/.cache/flox/downloads/postgres_sample.sql` by default
   - The download includes a progress spinner and size display
   - Use the `populate` command to create and populate the `iowa_liquor_sales` database
   - The populate script:
     - Creates the database if it doesn't exist
     - Creates the necessary table structure
     - Imports the data from the CSV
     - Converts date strings to proper DATE types
     - Creates indexes for better query performance

This sample dataset is perfect for exploring the PostgreSQL + Metabase integration, allowing you to visualize liquor sales data through Metabase's dashboards and analytics tools.

## 🔧 Troubleshooting

If you encounter issues:

1. **Connection fails**: 
   - Verify your PostgreSQL server is running with `flox services status postgres`
   - Check the logs of your PostgreSQL service with `flox services logs postgres`
   - Check your connection details in the PostgreSQL configuration
   
2. **Configuration issues**:
   - Run `pgconfigure` to reset and reconfigure your PostgreSQL instance
   - Check that your data directories have proper permissions

3. **Service startup problems**: 
   - Ensure ports aren't already in use by other services
   - Running `flox services logs postgres` could help with this

4. **Dataset loading issues**:
   - If the `fetch` command fails, check your internet connection?
   - Make you have sufficient disk space (~7.1GB) for the dataset
   - If the `populate` command fails, verify that PostgreSQL is running
   - Check permissions on the downloaded CSV file?

## 💻 System Compatibility

Our environment works on:
- macOS (ARM64, x86_64)
- Linux (ARM64, x86_64)

## About Flox

[Flox](https://flox.dev/docs) combines package and environment management, building on [Nix](https://github.com/NixOS/nix). It gives you Nix with a `git`-like syntax and an intuitive UX:

- **Declarative environments**. Software packages, variables, services, etc. are defined in simple, human-readable TOML format;
- **Content-addressed storage**. Multiple versions of packages with conflicting dependencies can coexist in the same environment;
- **Reproducibility**. The same environment can be reused across development, CI, and production;
- **Deterministic builds**. The same inputs always produce identical outputs for a given architecture, regardless of when or where builds occur;
- **World's largest collection of packages**. Access to over 150,000 packages—and millions of package-version combinations—from [Nixpkgs](https://github.com/NixOS/nixpkgs).
