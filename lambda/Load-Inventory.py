# Load-Inventory Lambda function
#
# Essa função é acionada quando um objeto é criado num bucket do S3.
# O arquivo é baixado e cada linha é inserida numa tabela do DynamoDB.

import json, urllib, boto3, csv

# Conectar ao S3 e DynamoDB
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

# Conectar à tabela do DynamoDB
inventoryTable = dynamodb.Table('Inventory');

# Esse handler e executado sempre que a função Lambda é acionada
def lambda_handler(event, context):
  # Exibe o evento no log de debug
  print("Event received by Lambda function: " + json.dumps(event, indent=2))

  # Bucket e a chave do objeto do evento
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
  localFilename = '/tmp/inventory.txt'
  # Baixa o arquivo do s3 para o localfile system
  try:
    s3.meta.client.download_file(bucket, key, localFilename)
  except Exception as e:
    print(e)
    print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
    raise e
  # Abre o arquivo CSV
  with open(localFilename) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    # Lê cada linha do arquivo
    rowCount = 0
    for row in reader:
      rowCount += 1
      # Mostra a linha no log de depuração
      print(row['store'], row['item'], row['count'])
      try:
        # Insere Store, Item e Count na tabela "Inventory"
        inventoryTable.put_item(
          Item={
            'Store':  row['store'],
            'Item':   row['item'],
            'Count':  int(row['count'])})
      except Exception as e:
         print(e)
         print("Unable to insert data into DynamoDB table".format(e))
    # Finished!
    return "%d counts inserted" % rowCount
