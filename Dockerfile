# Custom Dockerfile for Dagster Cloud
# This extends the base Python image to include git and system dependencies

FROM python:3.11-slim

# Install system dependencies including git
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# The rest of the setup (copying files, installing dependencies) is handled by Dagster Cloud
