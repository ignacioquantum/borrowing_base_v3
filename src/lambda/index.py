import json
from typing import Optional

import boto3

def lambda_handler(event, context):
    glue = boto3.client("glue")

    year = '2023'
    month = '11'
    day = '30'
    fx = 17.2333

    status = ['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED']
    status_detail_ignore = ['RESTRUCTURED', 'SPECIAL CONDITIONS']
    lease_id_list: Optional[list] = None

    # Invocación job
    glue.start_job_run(
        JobName="quantum_reports-borrowing_base", Arguments={"--file_path": year,
                                                             "--month": month,
                                                             "--day": day,
                                                             "--fx": fx,
                                                             "--status": status,
                                                             "--status_detail_ignore": status_detail_ignore,
                                                             "--lease_id_list": lease_id_list}
    )

    return {"statusCode": 200, "body": json.dumps("Ejecución del job de Glue iniciado con éxito")}
