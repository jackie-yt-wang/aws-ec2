## Extracting Job Data from The Muse API and Saving it to S3
### Description
This is a project to extract job data from The Muse API and save it as a CSV file in an S3 bucket. The project consists of a Python script, `muse-job-api.py`, that reads data from the API using the requests library, extracts the required data, manipulates it, and saves it as a CSV file. The `init.sh` is written on a EC2 Ubuntu instance to set up the `pipenv` virtual environment on your instance. Then the shell script, `run.sh`, runs the Python script, and uploads the resulting CSV file to an S3 bucket.

### Prerequisite to use this repo
- An AWS account
- An EC2 instance running Ubuntu with Python 3 installed
- AWS CLI installed on the EC2 instance (if not using `boto3`)
- `requests` and `pandas` libraries installed on the EC2 instance
- IAM Role with S3 permissions attached to the EC2 instance (if not using AWS CLI)
### How to use this repo
- Clone this repository to your local machine.
- Update the `config.toml` file with your API key and S3 bucket name.
- Copy the `init.sh` and `run.sh` files to your EC2 instance.
- Run the `init.sh` script to set up the virtual environment and install the necessary libraries.
- Make sure the `run.sh` script to include the correct paths to the `muse-job-api.py` and output directory.
- Run the `run.sh` script to execute the Python script and upload the resulting CSV file to S3.
### File Structure
- `log`: Directory to store log files (automatically created).
- `output`: Directory to store output CSV file (automatically created).
- `.gitignore`: File to specify files and directories to ignore in Git.
- `Pipfile` and `Pipfile.lock`: Files containing dependency information for Pipenv.
- `README.md`: This file.
- `config.toml`: File containing configuration information for the project.
- `init.sh`: Shell script to set up the virtual environment and install dependencies.
- `muse-job-api.py`: Python script to extract job data from The Muse API and save it as a CSV file.
- `run.sh`: Shell script to control the Python script and upload the output file to S3.

### Notes
If using boto3 to upload the CSV file to S3, you will need to set up appropriate IAM roles and permissions for your EC2 instance to access the S3 bucket.
