import boto3

glue = boto3.client('glue')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
eventbridge = boto3.client('events')
sqs = boto3.client('sqs')
sns = boto3.client('sns')
kms = boto3.client('kms')
lambda_client = boto3.client('lambda')
logs = boto3.client('logs')

def listar_databases():
    paginator = glue.get_paginator('get_databases')
    for page in paginator.paginate():
        for db in page['DatabaseList']:
            yield db['Name']

def listar_tabelas(database_name):
    paginator = glue.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        for tbl in page['TableList']:
            yield tbl['Name']

def deletar_tabelas(database_name):
    for table_name in listar_tabelas(database_name):
        print(f"Deletando tabela: {table_name} do database: {database_name}")
        glue.delete_table(DatabaseName=database_name, Name=table_name)


def deletar_database(database_name):
    print(f"Deletando database: {database_name}")
    glue.delete_database(Name=database_name)

def deletar_glue_catalog():
    for database_name in listar_databases():
        print(f"\nProcessando database: {database_name}")
        deletar_tabelas(database_name)
        deletar_database(database_name)


def deletar_s3():
    buckets = s3.buckets.all()
    for bucket in buckets:
        bucket_name = bucket.name
        print(f"\nLimpando e deletando bucket: {bucket_name}")

        # Deletar objetos versionados, se houver versionamento ativado

        versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
        if versioning.get('Status') == 'Enabled':
            for obj_version in s3.Bucket(bucket_name).object_versions.all():
                print(f"Deletando versão de objeto: {obj_version.object_key}")
                obj_version.delete()

        for obj in bucket.objects.all():
            print(f"Deletando objeto: {obj.key}")
            obj.delete()

        bucket.delete()
        print(f"Bucket {bucket_name} deletado com sucesso.")


def deletar_event_buses_personalizados():
    response = eventbridge.list_event_buses()

    for bus in response['EventBuses']:
        bus_name = bus['Name']

        if bus_name == 'default':
            print("Ignorando event bus padrão: default")
            continue

        print(f"Deletando event bus: {bus_name}")

        # Antes de deletar, é recomendado remover regras associadas
        regras = eventbridge.list_rules(EventBusName=bus_name)['Rules']
        for regra in regras:
            rule_name = regra['Name']
            print(f"  - Removendo regra: {rule_name}")

            # Remove targets (se houver) antes de deletar a regra
            targets = eventbridge.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name)['Targets']
            if targets:
                target_ids = [t['Id'] for t in targets]
                print(f"    - Removendo targets: {target_ids}")
                eventbridge.remove_targets(Rule=rule_name, EventBusName=bus_name, Ids=target_ids)

            # Deleta a regra
            eventbridge.delete_rule(Name=rule_name, EventBusName=bus_name, Force=True)

        # Agora deleta o event bus
        eventbridge.delete_event_bus(Name=bus_name)
        print(f"Event bus {bus_name} deletado com sucesso.")

def deletar_sqs():
    response = sqs.list_queues()
    queue_urls = response.get('QueueUrls', [])

    if not queue_urls:
        print("Nenhuma fila SQS encontrada.")
        return

    for queue_url in queue_urls:
        print(f"Deletando fila: {queue_url}")
        sqs.delete_queue(QueueUrl=queue_url)
        print(f"Fila {queue_url} deletada com sucesso.")

def deletar_sns():
    response = sns.list_topics()
    topics = response.get('Topics', [])

    if not topics:
        print("Nenhum tópico SNS encontrado.")
        return

    for topic in topics:
        topic_arn = topic['TopicArn']
        print(f"Processando tópico: {topic_arn}")

        # Listar e deletar todas as assinaturas do tópico
        subscriptions = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get('Subscriptions', [])
        for sub in subscriptions:
            sub_arn = sub['SubscriptionArn']
            if sub_arn != 'PendingConfirmation':
                print(f"  - Removendo assinatura: {sub_arn}")
                sns.unsubscribe(SubscriptionArn=sub_arn)

            sns.delete_topic(TopicArn=topic_arn)


def deletar_kms_custom_keys():
    response = kms.list_keys()
    chave_ids = [key['KeyId'] for key in response['Keys']]

    for key_id in chave_ids:
        # Obter detalhes da chave
        key_metadata = kms.describe_key(KeyId=key_id)['KeyMetadata']

        if key_metadata['KeyManager'] == 'CUSTOMER':
            print(f"\nProcessando chave KMS: {key_id}")

            # Desativar a chave (obrigatório antes de deletar)
            if key_metadata['Enabled']:
                kms.disable_key(KeyId=key_id)
                # Agendar exclusão (mínimo: 7 dias)

                if key_metadata['KeyState'] == 'Enabled':
                    key_deletion_response = kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
                    print(f"  - Exclusão agendada (7 dias)")
        else:
            print(f"Ignorando chave gerenciada pela AWS: {key_id}")

def deletar_lambda():
    paginator = lambda_client.get_paginator('list_functions')
    for page in paginator.paginate():
        for function in page['Functions']:
            function_name = function['FunctionName']
            print(f"Deletando função Lambda: {function_name}")
            lambda_client.delete_function(FunctionName=function_name)
            print(f"  - Função {function_name} deletada com sucesso.")

def deletar_log_groups():
    paginator = logs.get_paginator('describe_log_groups')
    for page in paginator.paginate():
        for log_group in page['logGroups']:
            log_group_name = log_group['logGroupName']
            print(f"Deletando log group: {log_group_name}")
            logs.delete_log_group(logGroupName=log_group_name)
            print(f"  - Log group {log_group_name} deletado com sucesso.")

if __name__ == "__main__":
    deletar_glue_catalog()
    deletar_event_buses_personalizados()
    deletar_s3()
    deletar_sqs()
    deletar_sns()
    deletar_kms_custom_keys()
    deletar_lambda()
    deletar_log_groups()










