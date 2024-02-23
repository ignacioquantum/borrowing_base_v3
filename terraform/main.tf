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
  utils_files    = [for item in fileset("../utils", "**/*") : item if length(regexall(local.ignore_folders, item)) == 0]
}

resource "aws_s3_object" "src" {
  for_each = toset(local.apps_files)
  bucket   = var.bucket_utility
  key      = "config_fin_skymetrics/src/${each.key}"
  source   = "../src/${each.key}"
  etag     = filemd5("../src/${each.key}")
}
resource "aws_s3_object" "utils" {
  for_each = toset(local.utils_files)
  bucket   = var.bucket_utility
  key      = "config_fin_skymetrics/utils/${each.key}"
  source   = "../utils/${each.key}"
  etag     = filemd5("../utils/${each.key}")
}

resource "aws_iam_role" "glue_role" {
  for_each           = var.job_skymetrics_config
  name               = each.value.role_name
  assume_role_policy = file("${path.module}/${each.value.role_file}")

  tags = local.tags
}

resource "aws_iam_role_policy" "glue_jobs" {
  for_each   = var.job_skymetrics_config
  name       = each.value.policy_name
  role       = aws_iam_role.glue_role[each.key].id
  policy     = file("${path.module}/${each.value.policy_file}")
  depends_on = [aws_iam_role.glue_role]
}
resource "aws_glue_job" "etl_skymetrics" {
  for_each          = var.job_skymetrics_config
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


resource "aws_iam_role" "step_functions" {
  for_each           = var.step_functions_config
  name               = each.value.role_name
  assume_role_policy = file("${path.module}/${each.value.role_file}")

  tags = local.tags
}

resource "aws_iam_policy" "step_functions" {
  for_each   = var.step_functions_config
  name       = each.value.policy_name
  policy     = file("${path.module}/${each.value.policy_file}")
  depends_on = [aws_iam_role.step_functions]
}

# Definir una función Lambda
resource "aws_lambda_function" "skymetrics_lambda" {
  for_each      = var.lambda_config
  filename      = each.value.file_name
  function_name = each.key
  role          = aws_iam_role.lambda_execution_role[each.key].arn

  handler = each.value.handler_name
  runtime = "python3.10"
}

resource "aws_cloudwatch_log_group" "lambda_skymetrics_log_group" {
  for_each          = var.lambda_config
  name              = "/aws/lambda/${each.key}"
  retention_in_days = each.value.log_retention
}

resource "aws_cloudwatch_log_group" "bdp-fin-skymetrics_log_group" {
  name              = "/aws-glue/bdp-fin-skymetrics"
  retention_in_days = 120
}
resource "aws_cloudwatch_log_group" "raw_data_log_group" {
  name              = "/aws-glue/jobs/bdp-fin-skymetrics/raw"
  retention_in_days = 120
}
resource "aws_cloudwatch_log_group" "final_data_log_group" {
  name              = "/aws-glue/jobs/bdp-fin-skymetrics/final"
  retention_in_days = 120
}
resource "aws_cloudwatch_log_group" "fuel_raw_data_log_group" {
  name              = "/aws-glue/jobs/bdp-fin-skymetrics/fuel_raw"
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

# Define la StepFunction
resource "aws_sfn_state_machine" "skymetrics_stepfunction" {
  for_each   = var.step_functions_config
  name       = each.key
  role_arn   = aws_iam_role.step_functions[each.key].arn
  definition = file("${path.module}/${each.value.template}")
  depends_on = [aws_glue_job.etl_skymetrics]
}

# Adjunta la política de IAM al rol de StepFunctions
resource "aws_iam_role_policy_attachment" "stepfunction_policy_attachment" {
  for_each   = var.step_functions_config
  policy_arn = aws_iam_policy.step_functions[each.key].arn
  role       = aws_iam_role.step_functions[each.key].name
}

resource "aws_ses_email_identity" "ses_skymetrics" {
  for_each = var.notifications_config
  email    = each.value.email # Reemplaza con la dirección de correo electrónico que quieras verificar
}

resource "aws_ses_identity_policy" "ses_skymetrics_policy" {
  for_each = var.notifications_config
  identity = aws_ses_email_identity.ses_skymetrics[each.key].id
  name     = each.key
  policy   = file("${path.module}/${each.value.policy_file}")
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

data "archive_file" "lambda_fuel_raw_zip" {
  type             = "zip"
  output_file_mode = "0666"
  output_path      = "../terraform/scripts/index_fuel_raw.zip"
  source {
    content  = file("../src/lambda/index_fuel_raw.py")
    filename = "index_fuel_raw.py"
  }
}

data "archive_file" "src_zip" {
  type             = "zip"
  output_file_mode = "0666"
  output_path      = "../src.zip"
  source_dir       = "../src"
}