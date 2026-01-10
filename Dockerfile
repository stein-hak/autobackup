FROM python:3.11-slim

LABEL maintainer="autobackup"
LABEL description="ZFS Autobackup Server - API-driven backup and replication"

# Set working directory
WORKDIR /opt/autobackup

# Copy application files
COPY requirements.txt backup_server.py backup_config.py zfs_api_client.py ./

# Install Python dependencies and create config directory
RUN pip install --no-cache-dir -r requirements.txt && \
    mkdir -p /opt/autobackup/config && \
    useradd -r -s /bin/false -u 1000 autobackup && \
    chown -R autobackup:autobackup /opt/autobackup

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV CONFIG_FILE=/opt/autobackup/config/backup_config.yaml

USER autobackup

# Health check - verify Python can import the module
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python3 -c "from backup_server import backup_server; print('OK')" || exit 1

# Run the backup server
CMD ["python3", "-u", "backup_server.py"]
