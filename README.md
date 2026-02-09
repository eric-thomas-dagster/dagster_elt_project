# Dagster ELT Project

**State-backed Dagster component for GitHub-hosted ELT pipelines**

Automatically discovers and orchestrates dlt and Sling pipelines from a GitHub repository, with scheduling, partitioning, and rich metadata support.

## 🎯 Key Features

✅ **GitHub Integration** - Clones/pulls pipelines from Git repositories
✅ **Auto-Discovery** - Finds all enabled dlt and Sling pipelines
✅ **Flexible Configuration** - YAML-based with environment variable support
✅ **Scheduling** - Automatic schedule creation from pipeline config
✅ **Partitioning** - Time-based and static partitions for incremental loading
✅ **Rich Metadata** - Comprehensive observability for Dagster+
✅ **Asset Groups** - Organize pipelines by team/domain
✅ **Retry Policies** - Built-in exponential backoff with jitter

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
cd dagster_elt_project
uv sync
```

### 2. Configure Environment

Create `.env` file:

```bash
# GitHub Repository Configuration
ELT_REPO_URL=https://github.com/your-org/elt-pipelines.git
ELT_REPO_BRANCH=main
GITHUB_TOKEN=ghp_your_token_here

# Pipeline Discovery
ELT_PIPELINES_DIR=pipelines
ELT_AUTO_REFRESH=true
```

### 3. Configure Component

Edit `dagster_elt_project/defs/elt_pipelines/defs.yaml`:

```yaml
type: dagster_elt_project.components.EltGithubComponent
attributes:
  repo_url: ${ELT_REPO_URL}
  repo_branch: ${ELT_REPO_BRANCH}
  github_token: ${GITHUB_TOKEN}
  pipelines_directory: ${ELT_PIPELINES_DIR}
  auto_refresh: ${ELT_AUTO_REFRESH}
```

### 4. Run Dagster

```bash
# Using uv
uv run dagster-elt-dev

# Or with custom port
uv run dagster-elt-dev 3000
```

Access the UI at: http://127.0.0.1:3000

---

## 📦 Pipeline Configuration

Each pipeline in your GitHub repo should have a `dagster.yaml` file:

### Example: Daily Partitioned Pipeline

```yaml
enabled: true
description: "Load GitHub issues incrementally by day"
group: "github"

owners:
  - "data-team@company.com"

# Time-based partitions for incremental loading
partitions:
  enabled: true
  type: "time"
  time:
    start: "2024-01-01"
    cron_schedule: "0 0 * * *"  # Daily
    timezone: "UTC"
    fmt: "%Y-%m-%d"

schedule:
  enabled: true
  cron_schedule: "0 2 * * *"
  timezone: "UTC"

tags:
  tool: "dlt"
  source: "github"
  team: "data-engineering"

retries: 3
retry_delay: 60
```

### Example: Static Partitions (by Region)

```yaml
enabled: true
description: "Extract Stripe payments by region"
group: "payments"

owners:
  - "finance-team@company.com"

partitions:
  enabled: true
  type: "static"
  static:
    partition_keys:
      - "us-east"
      - "us-west"
      - "eu-central"

schedule:
  enabled: true
  cron_schedule: "0 */4 * * *"

tags:
  source: "stripe"
  criticality: "high"

retries: 3
retry_delay: 120
```

---

## 🔧 Component Architecture

### State-Backed Design

The component uses Dagster's state-backed pattern:

1. **Write State** - Clones GitHub repo and discovers pipelines
2. **Build Definitions** - Creates Dagster assets, schedules, and jobs from state
3. **Auto-Refresh** - Optionally pulls latest changes on schedule

### Directory Structure

```
dagster_elt_project/
├── dagster_elt_project/
│   ├── components/
│   │   └── elt_github_component.py  # Main component
│   ├── schemas/
│   │   └── dagster_metadata.py      # YAML schema
│   └── defs/
│       └── elt_pipelines/
│           ├── defs.yaml            # Component config
│           ├── pipelines_state.json # Discovered pipelines
│           └── repo_clone/          # Cloned Git repo
├── .env                             # Environment config
└── pyproject.toml                   # Package definition
```

---

## 🎯 Features in Detail

### Partitioned Assets

**Benefits:**
- Incremental loading (process only new data)
- Efficient backfills (fill historical gaps)
- Parallel execution (process multiple partitions concurrently)
- Better observability (per-partition status)

**Types:**
- **Time partitions:** Daily, hourly, monthly incremental loads
- **Static partitions:** By region, environment, customer, etc.

### Asset Groups

Organize pipelines by:
- Team ownership (`github`, `crm`, `payments`)
- Data domain (`customer_data`, `financial`, `operational`)
- Environment (`dev`, `staging`, `production`)

### Retry Policies

Automatic retry with:
- Exponential backoff
- Jitter (randomization to prevent thundering herd)
- Configurable max retries and delay

### Rich Metadata

Emitted for every run:
- Execution timestamps
- Row and byte counts
- Throughput metrics
- Error details
- Partition information
- Pipeline configuration

---

## 🚀 Production Deployment

### Dagster+ (Cloud)

```bash
# Configure deployment
export DAGSTER_CLOUD_DEPLOYMENT=prod

# Deploy
dagster-cloud workspace deploy
```

### Docker

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . .

RUN pip install -e .

CMD ["dagster-elt-dev", "3000"]
```

### Kubernetes

Use the Dagster Helm chart with this project as the code location.

---

## 📊 Monitoring & Observability

### Dagster+ Features

- **Asset lineage** - Visual dependency graphs
- **Run timeline** - Execution history per partition
- **Alerting** - Email/Slack on failures
- **Insights** - Cost and performance analytics

### Metadata Display

All pipeline metadata appears in the Dagster UI:
- Custom tags for filtering
- Owner information for notifications
- Partition status grids
- Performance metrics per run

---

## 🔒 Security Best Practices

1. **Never commit secrets** - Use environment variables
2. **Rotate tokens** - Refresh GitHub tokens regularly
3. **Scope permissions** - Use read-only tokens when possible
4. **Audit access** - Monitor who runs which pipelines
5. **Encrypt at rest** - Use Dagster+ cloud for encrypted storage

---

## 🤝 Contributing

Pipeline developers should:

1. Add pipelines to your Git repository
2. Include `dagster.yaml` with proper configuration
3. Test locally with `uv run dagster-elt-dev`
4. Commit and push - pipelines auto-discover!

---

## 📚 Additional Resources

- [Dagster Documentation](https://docs.dagster.io)
- [dlt Documentation](https://dlthub.com/docs)
- [Sling Documentation](https://slingdata.io/docs)
- [Dagster Components Guide](https://docs.dagster.io/concepts/components)

---

## 🐛 Troubleshooting

### Pipelines Not Discovered

1. Check `.env` configuration
2. Verify GitHub token has repo access
3. Run `uv run dagster-elt-dev` and check logs
4. Ensure `dagster.yaml` files are valid

### Partitions Not Loading

1. Verify partition config in `dagster.yaml`
2. Check that `enabled: true` for partitions
3. Ensure date ranges are valid
4. Check Dagster logs for partition errors

### Schedule Not Running

1. Verify `schedule.enabled: true` in `dagster.yaml`
2. Check cron expression is valid
3. Ensure Dagster daemon is running
4. Check schedule status in Dagster UI

---

**Built with ❤️ using Dagster, dlt, and Sling**
