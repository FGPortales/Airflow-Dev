import json
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1, tzinfo=pendulum.timezone('America/Lima')),
}


class AttachmentFile:
    def __init__(self, name, url, file=None):
        self.name = name
        self.url = url
        self.file = file

    def has_name(self) -> bool:
        return False if not self.name else True


class Document:
    def __init__(self, folder, documentNumber, rucCustomer, attachmentFiles):
        self.folder = folder
        self.documentNumber = documentNumber
        self.rucCustomer = rucCustomer
        self.attachmentFiles = [AttachmentFile(
            data['name'], data['url'], data['file'] if data.get('file', None) else None) for data in attachmentFiles]

    def has_folder(self) -> bool:
        return False if not self.folder else True


class DocumentRequest:
    def __init__(self, data_documents: dict):
        self.documents = [Document(data["folder"], data["documentNumber"], data["rucCustomer"],
                                   data["attachmentFiles"]) for data in data_documents['documents']]


@task
def create_task_upload_documents_folder():
    context = get_current_context()
    request_document_conf = context['dag_run'].conf

    request_document = DocumentRequest(request_document_conf)

    return [json.dumps(document, default=lambda o: o.__dict__) for document in request_document.documents]


@task
def upload_individual_document_folder(document_json):
    from urllib.request import urlopen

    document = Document(**json.loads(document_json))

    if not document.has_folder():
        return AirflowFailException(f"El campo 'folder' está vacío. document: {document}")

    for j, attachment_file in enumerate(document.attachmentFiles):
        if not attachment_file.has_name():
            raise AirflowFailException(f"El campo 'name' está vacío. document {document}")

        try:
            with urlopen(attachment_file.url) as file:
                document.attachmentFiles[j].file = file.read()
                document.attachmentFiles[j].file = None
        except Exception as ex:
            raise AirflowFailException(f'No se pudo descargar el archivo URL: {attachment_file.url} - Exception: {ex}')

    return document.documentNumber


@task.branch(trigger_rule='all_done')
def validate_errors_document(documents_number_successful):

    context = get_current_context()
    request_document_conf = context['dag_run'].conf

    request_document = DocumentRequest(request_document_conf)

    failed_documents = []
    for document in request_document.documents:
        if document.documentNumber not in documents_number_successful:
            failed_documents.append(document.documentNumber)

    print('failed_documents: ', failed_documents)
    context['ti'].xcom_push(key='failed_documents', value=failed_documents)

    return 'bash_task' if failed_documents else 'none'


@task_group
def group_to_upload_files_documents():
    return upload_individual_document_folder.partial().expand(document_json=create_task_upload_documents_folder())


@dag(
    schedule=None,
    default_args=default_args,
    start_date=datetime(2021, 1, 1, tzinfo=pendulum.timezone('America/Lima')),
    catchup=False,
    max_active_runs=1,
    tags=['document']
)
def pre_register_document():

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Mensaje: \'{{ task_instance.xcom_pull(task_ids="validate_errors_document", key="failed_documents") }}\'"',
    )

    none = EmptyOperator(task_id='none')

    return_values = group_to_upload_files_documents()
    validate_errors_document(return_values) >> [bash_task, none]


pre_register_document()
