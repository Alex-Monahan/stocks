## MotherDuck Stocks Data Workshop

This workshop will help you get data from csv into MotherDuck, and lay out basic patterns for using dbt + MotherDuck in a performant way.

### Getting Started
1. Create a MotherDuck account.
2. Create a database called "stocks_dev" inside of MotherDuck. This can be done with the command `create database stocks_dev;` from the MotherDuck UI.
3. Fork the `matsonj/stocks` repo in GitHub.
4. Generate an access token inside of MotherDuck and add it as a codespace secret inside of GitHub.
5. Open a codespace on the repo.
6. After it loads completely, _reload the window_ in order to make sure the dbt power user extension has access to your md environment.

### Running the project
1. Build the data warehouse with `dbt build` in the CLI. The default dbt target is the MotherDuck-backed `dev` profile, and the project ships with example CSV snapshots as dbt seeds under `seeds/`.
2. Plot the results using `python3 viz/line_chart.py`. The webpage will be available at `127.0.0.1:8050`, and the app reads from `stocks_dev.main` in MotherDuck.
3. Alternatively, you can invoke these 2 steps with `make run`.
4. The Yahoo Finance helper scripts in `scripts/` append normalized rows directly into the stable seeds `seeds/ticker_info.csv`, `seeds/option_history.csv`, and `seeds/ticker_history.csv`. If a fetched row already exists, it is not duplicated.

### Data Flow Overview
1. Example data is stored as dbt seeds in `seeds/`, with `snapshot_ts` recording when each snapshot was fetched.
    - `symbols.txt` contains the list of symbols for which to fetch data.
    - `get_info.py` gets the company information for each company.
    - `get_options.py` gets the currently open options. *note:* this data is temporal, and thus needs to be snapshotted. This is left as an exercise to the reader.
    - `get_stock_history.py` gets the stock price history for the last 360 days.
    - Those scripts merge normalized snapshots into the stable seed files, so you can run the scripts and then run `dbt build`.
2. The raw dbt models `company_info.sql`, `options.sql`, and `stock_history.sql` reference those seed relations directly.
3. For the raw models in step 2, tests make sure that the primary key is unique.
4. The marts create a dataset of closing stock price X outstanding shares over time to estimate Market Cap.
   
### Plotting

1. Plotting is defined in the `viz/line_chart.py` file. It is a set of simple charts using `plotly` and `dash`. 
2. You can serve the plots with `python3 viz/line_chart.py`.

### Other Notes
1. In order to take advantage of the `dbt power user` plugin, you will need to put your `MOTHERDUCK_TOKEN` in your bash profile. Otherwise, all interactions with `dbt power user` will hit the login page for MotherDuck.
