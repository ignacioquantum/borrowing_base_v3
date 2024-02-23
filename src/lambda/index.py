import json
from datetime import datetime

import boto3

# Crea un cliente de AWS Step Functions
stepfunctions_client = boto3.client('stepfunctions')


def lambda_handler(event, context):
    # Recupera el nombre del bucket y el objeto S3 del evento
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']
    object_key = s3_event['object']['key']

    # Define el payload para la Step Function
    stepfunction_input = {
        "bucket": bucket_name,
        "objectKey": object_key
    }

    # Define el ARN de tu Step Function
    stepfunction_arn = 'arn:aws:states:us-east-1:account:stateMachine:sf_name'

    # Inicia la Step Function con el payload
    stepfunctions_client.start_execution(
        stateMachineArn=stepfunction_arn,
        # Nombre único para esta ejecución (puedes cambiarlo según tus necesidades)
        name=f"{datetime.now().strftime('%m_%d_%Y')}__{datetime.now().strftime('%H_%M_%S')}",
        # Convierte el diccionario de entrada en una cadena JSON
        input=json.dumps(stepfunction_input)
    )

    # Devuelve una respuesta para indicar que la ejecución de la Step Function se ha iniciado correctamente
    return {
        'statusCode': 200,
        'body': json.dumps('Ejecución de Step Function iniciada con éxito')
    }
