# Base image
FROM python:3

# Define working directory
WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy necessary files
COPY data_collector.py ./

# Entry point
ENTRYPOINT [ "python", "data_collector.py"]
