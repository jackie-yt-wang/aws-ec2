# Update system packages
sudo apt update && sudo apt upgrade -y

# Install Python 3
sudo apt install -y python3

# Install AWS CLI
sudo apt install -y awscli

# Install Pipenv
sudo apt install -y pipenv

# Create a new virtual environment and install project dependencies
pipenv install

# Set execute permission on run.py
chmod +x run.sh

# Create a log directory
mkdir -p log