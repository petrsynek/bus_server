# Set the base image using Python 3.12 and Debian Bookworm
FROM python:3.12-slim-bookworm

# Set the working directory to /app
WORKDIR /app

# Copy only the necessary files to the working directory
COPY requirements.txt /app/requirements.txt

# Install the requirements
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy the rest of the files to the working directory
COPY bus_server /app

# Expose the port the app runs on
EXPOSE 8000

WORKDIR /app

# Run the app
CMD ["litestar", "run", "--host", "0.0.0.0", "--port", "8000"]
