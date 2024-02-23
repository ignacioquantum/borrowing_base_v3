region                   = "us-east-1"
project_name             = "template"
environment              = "dev"
bucket_utility           = "dev-bucket"
bucket_origin            = "dev-origin"
topic_sns_name           = "notifications"
redshift_connection_name = "redshift"

jobs_config = {
  etl_job = {
    job_name              = "etl_job",
    glue_version          = "4.0",
    log_group             = "/aws-glue/jobs/etl_job",
    log_retention         = 300,
    role_name             = "etl_job_glue_role",
    role_file             = "roles/glue_role.json",
    policy_name           = "etl_job_glue_policy",
    policy_file           = "policies/dev/glue_policy.json",
    instance_type         = "G.1X",
    num_workers           = 10,
    max_concurrent_runs   = 1,
    script_location       = "s3://bucket_utility/template/src/glue/etl/driver.py",
    extra_py_files        = "s3://bucket_utility/template/src.zip"
    environment_variables = {
      "--ENV": "dev",
      "--LOG_GROUP_NAME": "/aws-glue/jobs/template",
      "--LOG_APP_NAME": "/aws-glue/jobs/template",
      "--ARN_ROLE_REDSHIFT": "arn:aws:iam::account:role",
      "--additional-python-modules": "redshift-connector, watchtower, openpyxl",
      "--secret_name": "secret"
    }
  }
}

step_functions_config = {
  sf = {
    template            = "step_functions/template.json",
    log_group           = "step_function",
    log_retention       = 300,
    role_name           = "step_functions_role",
    role_file           = "roles/step_functions_role.json",
    policy_name         = "step_functions_policy",
    policy_file         = "policies/dev/step_functions_policy.json",
    schedule_expression = "cron(0 10 * * ? *)"
  }
}

lambda_config = {
  lambda = {
    file_name = "scripts/index.zip",
    role_file   = "roles/lambda_role.json",
    policy_name = "lambda_policy",
    policy_file = "policies/dev/lambda_policy.json",
    handler_name = "index.lambda_handler",
    bucket_trigger = "dev-origin",
    arn_bucket = "arn:aws:s3:::dev-origin",
    log_retention = 60
  }
}

notifications_emails = ["dummy@quantumlease.com"]

notifications_config ={
  ses = {
    email = "quantum_dev@outlook.com",
    policy_file = "policies/dev/ses_policy.json"
  }
}
