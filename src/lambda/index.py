import json
import boto3

def lambda_handler(event, context):
    glue = boto3.client("glue")
    year = '2023'
    month = '11'
    day = '30'
    fx = 17.2333

    # Invocación job
    glue.start_job_run(
        JobName="quantum_reports-borrowing_base", Arguments={"--file_path": year,
                                                             "--month": month,
                                                             "--day": day,
                                                             "--fx": fx}
    )

    return {"statusCode": 200, "body": json.dumps("Ejecución del job de Glue iniciado con éxito")}
