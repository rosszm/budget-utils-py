# Budget Utils

This project allows me to pull my monthly rent/expenses data from a google spreadsheet that I share
with roommates. It also provides a script for estimating the expenses of a given month; This
is particularly useful for variable expenses, such as power and energy.


## Usage

First install the project from the pyproject.toml file:
```sh
pip install ./
```

Example usage:
```sh
# Set environment variables. These are used as default values for the update script.
export SPREADSHEET_ID=example-spreadsheet-id
export CLIENT_SECRET=./credentials/client_secret.json

# Pull data from google sheets
update-db --db /tmp/rent.db

# Estimate the expenses for the month of January 2020, split between 3 people.
estimate-rent "Jan 2020" 3 --db /tmp/rent.db
```

Use the `--help` option to see the full usage for each script.

### Testing

Tests can be run with:
```sh
python -m unittest
```