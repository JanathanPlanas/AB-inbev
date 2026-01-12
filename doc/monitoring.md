# Monitoring & Alerting Strategy

This document describes the monitoring and alerting approach for the Breweries Data Pipeline.

---

## Overview

The monitoring strategy covers three main areas:

1. **Pipeline Failures** - Task execution failures and errors
2. **Data Quality Issues** - Data validation and consistency checks
3. **Infrastructure Health** - System resources and service availability

---

## 1. Pipeline Monitoring

### 1.1 Airflow Built-in Monitoring

| Feature | Description |
|---------|-------------|
| **Task Status** | Track success/failure/retry status per task |
| **Execution Duration** | Monitor task execution times |
| **SLA Misses** | Alert when tasks exceed expected duration |
| **DAG Run History** | Historical execution logs |

### 1.2 Implementation in DAG

```python
default_args = {
    'retries': 3,                              # Automatic retries
    'retry_delay': timedelta(minutes=5),       # Delay between retries
    'retry_exponential_backoff': True,         # Exponential backoff
    'email_on_failure': True,                  # Email alerts
    'execution_timeout': timedelta(hours=1),   # Timeout protection
}
```

### 1.3 Failure Callbacks

```python
def on_failure_callback(context):
    """Send alert on task failure"""
    # Send to Slack, PagerDuty, email, etc.
    send_alert(
        channel='#data-alerts',
        message=f"Task {context['task_instance'].task_id} failed",
        severity='high'
    )
```

---

## 2. Data Quality Monitoring

### 2.1 Quality Checks by Layer

#### Bronze Layer
| Check | Description | Action |
|-------|-------------|--------|
| API Response Code | Verify 200 status | Retry or alert |
| Record Count | Compare with metadata total | Log warning if mismatch |
| Schema Validation | Ensure expected fields exist | Fail task |
| Empty Response | Detect empty API results | Alert |

#### Silver Layer
| Check | Description | Action |
|-------|-------------|--------|
| Row Count Comparison | Bronze vs Silver counts | Warn if >10% difference |
| Null Percentage | Monitor null values per column | Log metrics |
| Duplicate Check | Verify deduplication worked | Alert if duplicates found |
| Coordinate Validation | Check lat/long ranges | Log invalid counts |
| Partition Balance | Check partition sizes | Warn on small partitions |

#### Gold Layer
| Check | Description | Action |
|-------|-------------|--------|
| Aggregation Integrity | Sum(Gold) = Count(Silver) | Fail if mismatch |
| Zero Counts | Detect aggregations with 0 | Log warning |
| Completeness | All countries/states present | Alert if missing |

### 2.2 Implementation Example

```python
def validate_silver_quality(bronze_count: int, silver_count: int) -> bool:
    """Validate Silver layer data quality"""
    
    # Check retention rate
    retention_rate = silver_count / bronze_count if bronze_count > 0 else 0
    
    if retention_rate < 0.90:
        alert(f"High data loss: {(1 - retention_rate) * 100:.1f}%")
        return False
    
    return True
```

### 2.3 Great Expectations Integration (Recommended for Production)

```python
# Example expectation suite for Silver layer
expectation_suite = {
    "expectations": [
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
        {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "id"}},
        {"expectation_type": "expect_column_values_to_be_in_set", 
         "kwargs": {"column": "brewery_type", "value_set": ["micro", "nano", "brewpub", ...]}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "latitude", "min_value": -90, "max_value": 90}},
    ]
}
```

---

## 3. Alerting Configuration

### 3.1 Alert Channels

| Severity | Channel | Response Time |
|----------|---------|---------------|
| **Critical** | PagerDuty + Slack | Immediate |
| **High** | Slack #data-alerts | < 1 hour |
| **Medium** | Email digest | < 4 hours |
| **Low** | Dashboard only | Next business day |

### 3.2 Alert Examples

```
🚨 CRITICAL: Pipeline Failed
DAG: breweries_pipeline
Task: extract_bronze
Error: API Connection Timeout
Time: 2025-01-08 03:15:00 UTC
Action Required: Check API availability

⚠️ WARNING: Data Quality Issue
DAG: breweries_pipeline
Task: validate_pipeline
Issue: Silver record count 15% lower than Bronze
Bronze: 8,343 | Silver: 7,091
Action: Review transformation logs
```

### 3.3 Slack Integration (Example)

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def send_slack_alert(context):
    slack_msg = f"""
    :red_circle: *Pipeline Failed*
    *DAG*: {context['dag'].dag_id}
    *Task*: {context['task_instance'].task_id}
    *Time*: {context['execution_date']}
    """
    
    SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook',
        message=slack_msg
    ).execute(context)
```

---

## 4. Infrastructure Monitoring

### 4.1 Key Metrics

| Metric | Threshold | Alert |
|--------|-----------|-------|
| Airflow Scheduler Heartbeat | > 30s delay | Critical |
| PostgreSQL Connections | > 80% pool | Warning |
| Disk Usage (data volume) | > 85% | Warning |
| Memory Usage | > 90% | Critical |
| Container Health | Unhealthy | Critical |

### 4.2 Docker Health Checks

```yaml
# In docker-compose.yml
healthcheck:
  test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
  interval: 30s
  timeout: 10s
  retries: 5
```

### 4.3 Recommended Tools

| Tool | Purpose |
|------|---------|
| **Prometheus** | Metrics collection |
| **Grafana** | Dashboards and visualization |
| **Datadog** | Full observability platform |
| **CloudWatch** | AWS native monitoring |
| **Airflow Metrics** | Built-in StatsD integration |

---

## 5. Dashboard Recommendations

### 5.1 Operational Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│  BREWERIES PIPELINE - OPERATIONAL DASHBOARD                 │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Last Run: 2025-01-08 00:15:32 UTC    Status: ✅ SUCCESS    │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Bronze    │  │   Silver    │  │    Gold     │         │
│  │   8,343     │→ │   8,341     │→ │    1,247    │         │
│  │  records    │  │  records    │  │    rows     │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│                                                             │
│  Duration: 2m 34s    Data Loss: 0.02%    Partitions: 47    │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  Recent Runs:                                               │
│  • 2025-01-08 00:00 ✅ Success (2m 34s)                     │
│  • 2025-01-07 00:00 ✅ Success (2m 28s)                     │
│  • 2025-01-06 00:00 ⚠️ Warning - High latency (5m 12s)     │
│  • 2025-01-05 00:00 ✅ Success (2m 31s)                     │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Data Quality Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│  DATA QUALITY METRICS                                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Completeness:  ████████████████████░░░░  85%               │
│  Uniqueness:    ████████████████████████  100%              │
│  Validity:      ███████████████████████░  98%               │
│  Freshness:     ████████████████████████  < 24h             │
│                                                             │
│  Null Values by Column:                                     │
│  • phone:        ████████░░░░░░░░░░░░░░░  35%               │
│  • website_url:  ██████░░░░░░░░░░░░░░░░░  28%               │
│  • latitude:     ██░░░░░░░░░░░░░░░░░░░░░  8%                │
│  • longitude:    ██░░░░░░░░░░░░░░░░░░░░░  8%                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. Incident Response

### 6.1 Runbook

| Issue | Diagnosis | Resolution |
|-------|-----------|------------|
| API Timeout | Check openbrewerydb.org status | Wait and retry; increase timeout |
| Empty Bronze | Verify API response | Check API pagination logic |
| High Data Loss | Review transform logs | Check dedup/validation rules |
| Partition Failure | Check disk space | Clean old data; expand storage |
| Scheduler Down | Check container health | Restart scheduler container |

### 6.2 Escalation Path

```
Level 1: Automated retry (3 attempts)
    ↓ (if still failing)
Level 2: Slack alert to #data-alerts
    ↓ (if critical or no response in 30min)
Level 3: PagerDuty to on-call engineer
    ↓ (if no resolution in 1 hour)
Level 4: Escalate to team lead
```

---

## 7. Implementation Checklist

- [ ] Configure Airflow email alerts
- [ ] Set up Slack webhook integration
- [ ] Implement data quality checks in DAG
- [ ] Create Grafana dashboards
- [ ] Set up log aggregation (ELK/CloudWatch)
- [ ] Define SLAs for each task
- [ ] Create incident runbooks
- [ ] Test alert routing

---

## Summary

This monitoring strategy ensures:

1. **Visibility** - Real-time insights into pipeline health
2. **Proactivity** - Early detection of issues before impact
3. **Accountability** - Clear alerting and escalation paths
4. **Traceability** - Comprehensive logging for debugging

For production deployment, consider integrating with your organization's existing monitoring stack (Datadog, New Relic, Prometheus/Grafana, etc.).