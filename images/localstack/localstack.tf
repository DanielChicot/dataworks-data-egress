terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  access_key                  = "access_key_id"
  region                      = "eu-west-2"
  s3_force_path_style         = true
  secret_key                  = "secret_access_key"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3      = "http://localstack:4566"
    sqs      = "http://localstack:4566"
    dynamodb = "http://localstack:4566"
  }
}

resource "aws_sqs_queue" "integration_queue" {
  name = "integration-queue"
}

resource "aws_dynamodb_table" "data_egress" {
  name           = "data-egress"
  hash_key       = "source_prefix"
  range_key      = "pipeline_name"
  read_capacity  = 20
  write_capacity = 20

  attribute {
    name = "source_prefix"
    type = "S"
  }

  attribute {
    name = "pipeline_name"
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "opsmi_data_egress_config" {
  table_name = aws_dynamodb_table.data_egress.name
  hash_key   = aws_dynamodb_table.data_egress.hash_key
  range_key  = aws_dynamodb_table.data_egress.range_key

  item = <<ITEM
  {
    "source_prefix":          {"S":     "opsmi/"},
    "pipeline_name":          {"S":     "OpsMI"},
    "recipient_name":         {"S":     "OpsMI"},
    "transfer_type":          {"S":     "S3"},
    "source_bucket":          {"S":     "source"},
    "destination_bucket":     {"S":     "target"},
    "destination_prefix":     {"S":     "/"}
  }
  ITEM
}

resource "aws_dynamodb_table_item" "cbol_data_egress_config" {
  table_name = aws_dynamodb_table.data_egress.name
  hash_key   = aws_dynamodb_table.data_egress.hash_key
  range_key  = aws_dynamodb_table.data_egress.range_key

  item = <<ITEM
  {
    "source_prefix":          {"S":     "dataegress/cbol-report/$TODAYS_DATE/"},
    "pipeline_name":          {"S":     "CBOL"},
    "recipient_name":         {"S":     "CBOL"},
    "transfer_type":          {"S":     "S3"},
    "source_bucket":          {"S":     "source"},
    "destination_bucket":     {"S":     "target"},
    "destination_prefix":     {"S":     "cbol/"}
  }
  ITEM
}

resource "aws_s3_bucket" "source_bucket" {
  bucket = "source"
  acl    = "public-read"
}

resource "aws_s3_bucket" "target_bucket" {
  bucket = "target"
  acl    = "public-read"
}
