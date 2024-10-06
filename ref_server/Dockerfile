# Set the base image using Python 3.12 and Debian Bookworm
FROM python:3.12-slim-bookworm

# Set the working directory to /app
WORKDIR /app

# Copy only the necessary files to the working directory
COPY requirements.txt /app/requirements.txt

# Install the requirements
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy the ref server requirements
COPY requirements_dev.txt /app/requirements_dev.txt

# Install the ref server requirements
RUN pip install --no-cache-dir --upgrade -r /app/requirements_dev.txt

# Copy the rest of the files to the working directory
COPY ref_server /app

# Expose the port the app runs on
EXPOSE 8080

WORKDIR /app

# Run the app
# CMD ["ls", "-la"]
CMD ["litestar", "run", "--host", "0.0.0.0", "--port", "8080"]
