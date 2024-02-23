terraform {
  backend "s3" {
    region  = "us-east-1"
    encrypt = true
  }
}
provider "aws" {
  region = "us-east-1"
}

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
  ignore_folders = "tests/|resources|.pytest|__pycache__|.DS_Store"
  apps_files     = [for item in fileset("../src", "**/*") : item if length(regexall(local.ignore_folders, item)) == 0]
}

resource "aws_s3_object" "src" {
  for_each = toset(local.apps_files)
  bucket   = var.bucket_utility
  key      = "config_quantum_reports_borrowing_base/src/${each.key}"
  source   = "../src/${each.key}"
  etag     = filemd5("../src/${each.key}")
}

resource "aws_iam_role" "glue_role" {
  for_each           = var.jobs_config
  name               = each.value.role_name
  assume_role_policy = file("${path.module}/${each.value.role_file}")

  tags = local.tags
}

resource "aws_iam_role_policy" "glue_jobs" {
  for_each   = var.jobs_config
  name       = each.value.policy_name
  role       = aws_iam_role.glue_role[each.key].id
  policy     = file("${path.module}/${each.value.policy_file}")
  depends_on = [aws_iam_role.glue_role]
}
resource "aws_glue_job" "borrowing_base_job" {
  for_each          = var.jobs_config
  name              = each.value.job_name
  role_arn          = aws_iam_role.glue_role[each.key].arn
  glue_version      = each.value.glue_version
  number_of_workers = each.value.num_workers
  worker_type       = each.value.instance_type

  execution_property {
    max_concurrent_runs = each.value.max_concurrent_runs
  }

  command {
    script_location = each.value.script_location
  }

  default_arguments = merge(
    {
      "--continuous-log-logGroup"          = each.value.log_group
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-continuous-log-filter"     = "true"
      "--enable-metrics"                   = ""
      "--extra-py-files"                   = each.value.extra_py_files
      "--job-language"                     = "python"
    },
    { for key, value in each.value.environment_variables : key => value }
  )

  tags = local.tags
}

# Definir una función Lambda
resource "aws_lambda_function" "borrowing_base_lambda" {
  for_each      = var.lambda_config
  filename      = each.value.file_name
  function_name = each.key
  role          = aws_iam_role.lambda_execution_role[each.key].arn

  handler = each.value.handler_name
  runtime = "python3.10"
}

resource "aws_cloudwatch_log_group" "lambda_borrowing_base_log_group" {
  for_each          = var.lambda_config
  name              = "/aws/lambda/${each.key}"
  retention_in_days = each.value.log_retention
}

resource "aws_cloudwatch_log_group" "borrowing_base_log_group" {
  name              = "/aws-glue/bdp-fin-skymetrics"
  retention_in_days = 120
}

# Define el rol de IAM para la ejecución de la función Lambda
resource "aws_iam_role" "lambda_execution_role" {
  for_each           = var.lambda_config
  name               = each.value.policy_name
  assume_role_policy = file("${path.module}/${each.value.role_file}")
}

# Define la política de IAM para permitir que Lambda ejecute Glue y StepFunctions
resource "aws_iam_policy" "lambda_policy" {
  for_each = var.lambda_config
  name     = each.value.policy_name
  policy   = file("${path.module}/${each.value.policy_file}")
}

# Adjunta la política de IAM al rol de Lambda
resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
  for_each   = var.lambda_config
  policy_arn = aws_iam_policy.lambda_policy[each.key].arn
  role       = aws_iam_role.lambda_execution_role[each.key].name
  depends_on = [aws_iam_policy.lambda_policy]
}

data "archive_file" "lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  output_path      = "../terraform/scripts/index.zip"
  source {
    content  = file("../src/lambda/index.py")
    filename = "index.py"
  }
}
