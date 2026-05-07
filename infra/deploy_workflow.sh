#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# GlobalPartners Glue Workflow — Deploy Script
# Run this to recreate the workflow from scratch in any environment
# Usage: bash infra/deploy_workflow.sh
# ═══════════════════════════════════════════════════════════════

set -e  # Exit on any error

WORKFLOW_NAME="gp-data-pipeline"

echo "Creating workflow: $WORKFLOW_NAME"

# ── Create workflow ─────────────────────────────────────────────
aws glue create-workflow \
  --name $WORKFLOW_NAME \
  --description "GlobalPartners pipeline: Bronze → Silver → Gold"

# ── Trigger 1: Schedule → Bronze ingestion ──────────────────────
echo "Creating trigger: gp-trigger-start"
aws glue create-trigger \
  --name gp-trigger-start \
  --workflow-name $WORKFLOW_NAME \
  --type SCHEDULED \
  --schedule "cron(0 2 * * ? *)" \
  --actions '[{"JobName": "gp-bronze-ingestion"}]' \
  --start-on-creation

# ── Trigger 2: Bronze job → Bronze crawler ──────────────────────
echo "Creating trigger: gp-trigger-bronze-crawler"
aws glue create-trigger \
  --name gp-trigger-bronze-crawler \
  --workflow-name $WORKFLOW_NAME \
  --type CONDITIONAL \
  --predicate '{
    "Logical": "AND",
    "Conditions": [
      {
        "LogicalOperator": "EQUALS",
        "JobName": "gp-bronze-ingestion",
        "State": "SUCCEEDED"
      }
    ]
  }' \
  --actions '[{"CrawlerName": "gp-bronze-crawler"}]' \
  --start-on-creation

# ── Trigger 3: Bronze crawler → Silver job ──────────────────────
echo "Creating trigger: gp-trigger-silver"
aws glue create-trigger \
  --name gp-trigger-silver \
  --workflow-name $WORKFLOW_NAME \
  --type CONDITIONAL \
  --predicate '{
    "Logical": "AND",
    "Conditions": [
      {
        "LogicalOperator": "EQUALS",
        "CrawlerName": "gp-bronze-crawler",
        "CrawlState": "SUCCEEDED"
      }
    ]
  }' \
  --actions '[{"JobName": "gp-silver-transform"}]' \
  --start-on-creation

# ── Trigger 4: Silver job → Silver crawler ──────────────────────
echo "Creating trigger: gp-trigger-silver-crawler"
aws glue create-trigger \
  --name gp-trigger-silver-crawler \
  --workflow-name $WORKFLOW_NAME \
  --type CONDITIONAL \
  --predicate '{
    "Logical": "AND",
    "Conditions": [
      {
        "LogicalOperator": "EQUALS",
        "JobName": "gp-silver-transform",
        "State": "SUCCEEDED"
      }
    ]
  }' \
  --actions '[{"CrawlerName": "gp-silver-crawler"}]' \
  --start-on-creation

# ── Trigger 5: Silver crawler → All Gold jobs (parallel) ────────
echo "Creating trigger: gp-trigger-gold"
aws glue create-trigger \
  --name gp-trigger-gold \
  --workflow-name $WORKFLOW_NAME \
  --type CONDITIONAL \
  --predicate '{
    "Logical": "AND",
    "Conditions": [
      {
        "LogicalOperator": "EQUALS",
        "CrawlerName": "gp-silver-crawler",
        "CrawlState": "SUCCEEDED"
      }
    ]
  }' \
  --actions '[
    {"JobName": "gp-gold-clv"},
    {"JobName": "gp-gold-rfm"},
    {"JobName": "gp-gold-sales-trends"},
    {"JobName": "gp-gold-churn-indicators"},
    {"JobName": "gp-gold-loyalty"},
    {"JobName": "gp-gold-location"},
    {"JobName": "gp-gold-discount"}
  ]' \
  --start-on-creation

# ── Trigger 6: All Gold jobs → Gold crawlers ────────────────────
echo "Creating trigger: gp-trigger-gold-crawler"
aws glue create-trigger \
  --name gp-trigger-gold-crawler \
  --workflow-name $WORKFLOW_NAME \
  --type CONDITIONAL \
  --predicate '{
    "Logical": "AND",
    "Conditions": [
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-clv",              "State": "SUCCEEDED"},
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-rfm",              "State": "SUCCEEDED"},
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-sales-trends",     "State": "SUCCEEDED"},
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-churn-indicators", "State": "SUCCEEDED"},
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-loyalty",          "State": "SUCCEEDED"},
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-location",         "State": "SUCCEEDED"},
      {"LogicalOperator": "EQUALS", "JobName": "gp-gold-discount",         "State": "SUCCEEDED"}
    ]
  }' \
  --actions '[
    {"CrawlerName": "gp-gold-crawler"},
    {"CrawlerName": "gp-gold-churn-crawler"}
  ]' \
  --start-on-creation

echo "✅ Workflow $WORKFLOW_NAME created successfully"
echo "   View at: https://console.aws.amazon.com/glue/home#/etl/orchestration/workflows"