# ZFS Autobackup Server

Automated ZFS snapshot management and remote replication system using the ZFS JSON-RPC API.

## Features

- 🔄 **Automated Snapshots** - Configurable retention policies (frequent, hourly, daily, weekly, monthly, yearly)
- 🌐 **Remote Replication** - Async multi-destination backup support with progress tracking
- 📊 **API-Driven** - Uses ZFS API for all operations (no direct ZFS commands)
- 🔧 **Unified Config** - Single YAML file for all settings
- 🐳 **Docker Ready** - Containerized deployment with Docker Compose
- 📈 **Hold-Based Tracking** - Tracks sync status via ZFS snapshot holds
- ⏱️ **UTC Snapshot Names** - Consistent naming across distributed timezones

## Architecture

```
backup_server.py  →  zfs_api_client.py  →  ZFS API (localhost:8545)  →  ZFS Commands
     ↓
backup_config.py  →  backup_config.yaml
```

## Prerequisites

- **ZFS API Server** running on `localhost:8545` (from [stein-hak/zfs-api](https://github.com/stein-hak/zfs-api))
- Python 3.11+ (for native) or Docker (for containerized)
- ZFS-enabled system

## Quick Start

### 1. Clone Repository

```bash
git clone git@github.com:stein-hak/autobackup.git
cd autobackup
```

### 2. Configure

```bash
# Copy example config
cp backup_config.yaml.example backup_config.yaml

# Edit configuration
nano backup_config.yaml
```

### 3. Verify ZFS API

```bash
curl http://localhost:8545/health
```

### 4. Deploy

**Docker (Recommended):**
```bash
docker-compose up --build -d
docker-compose logs -f
```

**Native Python:**
```bash
pip install -r requirements.txt
python3 backup_server.py
```

## Configuration

### backup_config.yaml

Single unified configuration file:

```yaml
# Server settings
server:
  backup_interval: 600  # seconds (10 min)
  schedule:
    days: "1111111"     # Mon-Sun (1=enabled)
    hours: "111111111111111111111111"  # 24 hours
  retention:
    frequent: 4
    hourly: 12
    daily: 7
    weekly: 4
    monthly: 6
    yearly: 3
  remote_sync:
    enabled: true
    interval: 86400     # seconds (24 hours)
    days: "1111111"
    hours: "111111111111111111111111"

# ZFS API connection
zfs_api:
  url: "http://localhost:8545"
  timeout: 30

# Datasets to backup
datasets:
  - local_dataset: "pool/dataset"
    destinations:
      - remote_host: "192.168.1.100"
        remote_dataset: "backup/dataset"
        enabled: true
```

### Configuration Validation

Test config before deploying (like `nginx -t`):

```bash
python3 test-config.py backup_config.yaml
```

### Config Reload Behavior

**Auto-reloads every 60 seconds:**
- ✅ Dataset changes (add/remove, enable/disable destinations)
- ❌ Server settings (retention, intervals, schedules) - requires restart

**Atomic reload:** Old config preserved if YAML is corrupted. Safe to edit while running.

## How It Works

### Snapshot Creation

1. Server checks schedule (days/hours configuration)
2. Determines snapshot type based on time elapsed:
   - **Yearly** - Year changed
   - **Monthly** - Month changed
   - **Weekly** - Week changed
   - **Daily** - Day changed
   - **Hourly** - Hour changed
   - **Frequent** - Backup interval elapsed (default: 10 min)
3. Creates snapshot via API: `dataset@type_backup_YYYY-MM-DD-HH-MM` (UTC)
4. Old snapshots cleaned up based on retention policy

### Remote Replication

1. Server checks remote sync schedule
2. For each destination:
   - Checks last sync time (from holds)
   - If interval elapsed, starts async migration via API
   - API handles incremental/full send automatically
   - Places hold on snapshot: `sync_YYYY-MM-DD-HH-MM-SS_hostname` (local timezone)
3. Monitors migration status in background with progress logging
4. Old sync holds cleaned up (keeps latest per host)

### Hold-Based Tracking

- **Snapshot holds** prevent deletion and store metadata
- **Format**: `sync_<timestamp>_<hostname>`
- System parses holds to determine last successful sync per destination
- Only latest hold per host is kept to allow snapshot cleanup

### UTC vs Local Time

- **Snapshot names**: UTC time (consistent across distributed setups)
- **Hold timestamps**: Local server timezone (created by ZFS API)
- **Comparisons**: Local timezone (container syncs with host via `/etc/localtime`)

## File Structure

```
/opt/autobackup/
├── backup_server.py           # Main server (Thread-based loop)
├── backup_config.py           # Config parser (ServerConfig, BackupConfig)
├── backup_config.yaml         # Unified configuration
├── backup_config.yaml.example # Template for new deployments
├── zfs_api_client.py          # ZFS API client wrapper
├── test-config.py             # Config validation tool
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Container definition
├── docker-compose.yml         # Docker Compose config
└── .dockerignore              # Docker ignore rules
```

## Operations

### Update Deployment

```bash
cd /opt/autobackup
git pull origin main
docker-compose down
docker-compose up --build -d
```

### Configuration Changes

**Dataset changes only:**
- Edit `backup_config.yaml`
- Auto-reloads in 60 seconds (no restart needed)

**Server settings:**
- Edit `backup_config.yaml`
- Restart: `docker-compose restart`

### Monitoring

```bash
# Container logs
docker-compose logs -f

# Recent snapshots
zfs list -t snapshot -r pool/dataset | tail -20

# Sync holds
zfs holds pool/dataset@snapshot_name

# Remote backups
ssh remote-host "zfs list -t snapshot -r backup/dataset"
```

## Troubleshooting

### Container won't start

```bash
# Check ZFS API
curl http://localhost:8545/health

# Check logs
docker-compose logs

# Validate config
python3 test-config.py backup_config.yaml
```

### Snapshots not created

```bash
# Check schedule in config (days/hours)
# Verify dataset exists
zfs list pool/dataset

# Check container logs
docker-compose logs --tail 100
```

### Remote sync not working

```bash
# Test remote host connectivity (ZFS API handles SSH)
ssh user@remote-host

# Check migration status via API
curl -X POST http://localhost:8545/ \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"migration_list","id":1}'

# Check container logs for migration progress
docker-compose logs -f | grep Migration
```

## Security

- Container runs as non-root user (UID 1000)
- Config file mounted read-only
- ZFS API handles SSH authentication (not this container)
- No direct ZFS commands in container (all via API)
- Timezone files mounted read-only

## Dependencies

- **ZFS API**: [stein-hak/zfs-api](https://github.com/stein-hak/zfs-api)
- **Python**: PyYAML, requests (see requirements.txt)

## License

Internal use

## Support

For issues or questions:
- ZFS API documentation: [zfs-api repository](https://github.com/stein-hak/zfs-api)
- Configuration reference in this README
- Docker logs for error messages
