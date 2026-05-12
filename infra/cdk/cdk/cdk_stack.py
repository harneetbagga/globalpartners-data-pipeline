from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_glue as glue,
    aws_secretsmanager as secretsmanager,
    RemovalPolicy,
    Duration,
    CfnOutput,
)
from constructs import Construct


class GlobalPartnersStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ══════════════════════════════════════════════════════════
        # S3 — Data Lake Bucket
        # ══════════════════════════════════════════════════════════
        data_lake = s3.Bucket(
            self, "DataLakeBucket",
            bucket_name        = "gp-data-lake-dev",
            versioned          = False,
            removal_policy     = RemovalPolicy.RETAIN,
            block_public_access= s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules    = [
                # Move Bronze to IA after 90 days
                s3.LifecycleRule(
                    id         = "bronze-to-ia",
                    prefix     = "bronze/",
                    transitions= [
                        s3.Transition(
                            storage_class          = s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after       = Duration.days(90),
                        )
                    ],
                ),
                # Move DLQ to Glacier after 30 days
                s3.LifecycleRule(
                    id         = "dlq-to-glacier",
                    prefix     = "dlq/",
                    transitions= [
                        s3.Transition(
                            storage_class    = s3.StorageClass.GLACIER,
                            transition_after = Duration.days(30),
                        )
                    ],
                ),
            ],
        )

        # Create standard S3 prefixes
        for prefix in [
            "bronze/", "silver/", "gold/", "scripts/",
            "temp/", "dlq/", "athena-results/", "spark-logs/",
        ]:
            s3.CfnBucket.CorsConfigurationProperty
            pass  # Prefixes are created by jobs, not CDK

        # ══════════════════════════════════════════════════════════
        # DynamoDB — Watermarks Table
        # ══════════════════════════════════════════════════════════
        watermarks_table = dynamodb.Table(
            self, "WatermarksTable",
            table_name     = "gp_watermarks",
            partition_key  = dynamodb.Attribute(
                name="table_name",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode   = dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy = RemovalPolicy.RETAIN,
        )

        # ══════════════════════════════════════════════════════════
        # Secrets Manager — RDS Credentials
        # ══════════════════════════════════════════════════════════
        rds_secret = secretsmanager.Secret(
            self, "RdsSecret",
            secret_name        = "gp/sqlserver/pipeline",
            description        = "GlobalPartners SQL Server RDS credentials",
            generate_secret_string = secretsmanager.SecretStringGenerator(
                secret_string_template = '{"username": "admin"}',
                generate_string_key    = "password",
                exclude_characters     = '"@/\\',
            ),
        )

        # ══════════════════════════════════════════════════════════
        # IAM — Glue Execution Role
        # ══════════════════════════════════════════════════════════
        glue_role = iam.Role(
            self, "GlueExecutionRole",
            role_name   = "GlueGPExecutionRole",
            assumed_by  = iam.ServicePrincipal("glue.amazonaws.com"),
            description = "Execution role for GlobalPartners Glue jobs",
            managed_policies = [
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchLogsFullAccess"
                ),
            ],
        )

        # S3 full access on data lake
        data_lake.grant_read_write(glue_role)

        # DynamoDB access for watermarks
        watermarks_table.grant_read_write_data(glue_role)

        # Secrets Manager read access
        rds_secret.grant_read(glue_role)

        # ══════════════════════════════════════════════════════════
        # Glue — Databases
        # ══════════════════════════════════════════════════════════
        bronze_db = glue.CfnDatabase(
            self, "BronzeDatabase",
            catalog_id     = self.account,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name        = "gp_bronze",
                description = "GlobalPartners Bronze layer",
            ),
        )

        silver_db = glue.CfnDatabase(
            self, "SilverDatabase",
            catalog_id     = self.account,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name        = "gp_silver",
                description = "GlobalPartners Silver layer",
            ),
        )

        gold_db = glue.CfnDatabase(
            self, "GoldDatabase",
            catalog_id     = self.account,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name        = "gp_gold",
                description = "GlobalPartners Gold layer",
            ),
        )

        # ══════════════════════════════════════════════════════════
        # Glue — Crawlers
        # ══════════════════════════════════════════════════════════
        bronze_crawler = glue.CfnCrawler(
            self, "BronzeCrawler",
            name          = "gp-bronze-crawler",
            role          = glue_role.role_arn,
            database_name = "gp_bronze",
            targets       = glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/bronze/order_items/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/bronze/order_item_options/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/bronze/date_dim/"
                    ),
                ]
            ),
            recrawl_policy = glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            ),
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        silver_crawler = glue.CfnCrawler(
            self, "SilverCrawler",
            name          = "gp-silver-crawler",
            role          = glue_role.role_arn,
            database_name = "gp_silver",
            targets       = glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/silver/order_items/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/silver/order_item_options/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/silver/date_dim/"
                    ),
                ]
            ),
            recrawl_policy = glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            ),
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        gold_crawler = glue.CfnCrawler(
            self, "GoldCrawler",
            name          = "gp-gold-crawler",
            role          = glue_role.role_arn,
            database_name = "gp_gold",
            targets       = glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/clv/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/rfm/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/sales_trends_daily/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/sales_trends_weekly/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/sales_trends_monthly/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/loyalty_summary/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/loyalty_by_category/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/loyalty_monthly_trend/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/location_performance/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/location_monthly_performance/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/location_top_items/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/discount_order_summary/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/discount_by_category/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/discount_customer_behaviour/"
                    ),
                ]
            ),
            recrawl_policy = glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            ),
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        gold_churn_crawler = glue.CfnCrawler(
            self, "GoldChurnCrawler",
            name          = "gp-gold-churn-crawler",
            role          = glue_role.role_arn,
            database_name = "gp_gold",
            targets       = glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path="s3://gp-data-lake-dev/gold/churn_indicators/"
                    ),
                ]
            ),
            recrawl_policy = glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            ),
            schema_change_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        # ══════════════════════════════════════════════════════════
        # Glue — Jobs
        # ══════════════════════════════════════════════════════════
        glue_job_defaults = {
            "role"         : glue_role.role_arn,
            "glue_version" : "4.0",
            "worker_type"  : "G.1X",
            "number_of_workers": 2,
            "default_arguments": {
                "--TempDir"                        : "s3://gp-data-lake-dev/temp/",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics"                  : "true",
            },
        }

        jobs = [
            ("BronzeIngestion",       "gp-bronze-ingestion",       "bronze_ingestion.py"),
            ("SilverTransform",       "gp-silver-transform",       "silver_transform.py"),
            ("GoldClv",               "gp-gold-clv",               "gold_clv.py"),
            ("GoldRfm",               "gp-gold-rfm",               "gold_rfm.py"),
            ("GoldSalesTrends",       "gp-gold-sales-trends",      "gold_sales_trends.py"),
            ("GoldChurnIndicators",   "gp-gold-churn-indicators",  "gold_churn_indicators.py"),
            ("GoldLoyalty",           "gp-gold-loyalty",           "gold_loyalty_comparison.py"),
            ("GoldLocation",          "gp-gold-location",          "gold_location_performance.py"),
            ("GoldDiscount",          "gp-gold-discount",          "gold_discount_effectiveness.py"),
        ]

        for construct_id, job_name, script_file in jobs:
            glue.CfnJob(
                self, f"GlueJob{construct_id}",
                name    = job_name,
                role    = glue_role.role_arn,
                command = glue.CfnJob.JobCommandProperty(
                    name            = "glueetl",
                    script_location = f"s3://gp-data-lake-dev/scripts/{script_file}",
                    python_version  = "3",
                ),
                glue_version       = "4.0",
                worker_type        = "G.1X",
                number_of_workers  = 2,
                default_arguments  = {
                    "--TempDir"                        : "s3://gp-data-lake-dev/temp/",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--enable-metrics"                  : "true",
                },
            )

        # ══════════════════════════════════════════════════════════
        # CloudFormation Outputs
        # ══════════════════════════════════════════════════════════
        CfnOutput(self, "DataLakeBucketName",
                  value=data_lake.bucket_name,
                  description="S3 Data Lake bucket")

        CfnOutput(self, "GlueRoleArn",
                  value=glue_role.role_arn,
                  description="Glue execution role ARN")

        CfnOutput(self, "WatermarksTableName",
                  value=watermarks_table.table_name,
                  description="DynamoDB watermarks table")