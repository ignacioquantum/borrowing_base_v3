region                   = "us-east-1"
project_name             = "quantum_reports-borrow_base"
environment              = "prod"
bucket_utility           = "prod-bucket"

jobs_config = {
  quantum_reports-borrow_base_job = {
    job_name              = "quantum_reports-borrow_base_job",
    glue_version          = "4.0",
    log_group             = "/aws-glue/jobs/quantum_reports-borrow_base_job",
    log_retention         = 300,
    role_name             = "quantum_reports-borrow_base_job_glue_role",
    role_file             = "roles/glue_role.json",
    policy_name           = "quantum_reports-borrow_base_job_glue_policy",
    policy_file           = "policies/prod/glue_policy.json",
    instance_type         = "G.1X",
    num_workers           = 10,
    max_concurrent_runs   = 1,
    script_location       = "s3://bucket_utility/borrow_base/src/glue/borrow_base_v3/driver.py",
    extra_py_files        = "s3://bucket_utility/borrow_base/src.zip"
    environment_variables = {
      "--ENV": "prod",
      "--LOG_GROUP_NAME": "/aws-glue/jobs/borrow_base_v3",
      "--LOG_APP_NAME": "/aws-glue/jobs/borrow_base_v3",
      "--additional-python-modules": "redshift-connector, watchtower, openpyxl",
      "--secret_name": "secret"
    }
  }
}

lambda_config = {
  lambda = {
    file_name = "scripts/index.zip",
    role_file   = "roles/lambda_role.json",
    policy_name = "lambda_policy",
    policy_file = "policies/prod/lambda_policy.json",
    handler_name = "index.lambda_handler",
    bucket_trigger = "prod-origin",
    arn_bucket = "arn:aws:s3:::prod-origin",
    log_retention = 60
  }
}
