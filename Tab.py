import tableauserverclient as TSC
from tableauserverclient import ServerResponseError 
import os
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection as Connection_tab, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException, \
    TypeTag, \
    HyperException
import logging
from pathlib import Path    
import sys

mapper = {
'timestamp': 'SqlType.timestamp()',
'varchar': 'SqlType.text()',
'integer': 'SqlType.big_int()',
'numeric': 'SqlType.double()',
'double': 'SqlType.double()',
'date': 'SqlType.date()'
}

logger = logging.getLogger(__name__)
CA=os.path.dirname(os.path.realpath(__file__))+'/certs/CA.pem'

def createTabTable(tableName,columnHeading,dataTypes):
    extract_table = TableDefinition(table_name=TableName("Extract",tableName))
    
    for head in columnHeading:
        extract_table.add_column(head,eval(mapper[dataTypes.pop(0)]))

    return extract_table
        
def createHyperExtract(hyperLocation,extract_table,records,tableName):
    path_to_database = Path(hyperLocation)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection_tab(endpoint=hyper.endpoint,database=path_to_database,create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
            connection.catalog.create_table(table_definition=extract_table)

            with Inserter(connection, extract_table) as inserter:
                inserter.add_rows(rows=records)
                inserter.execute()
	    
            table_names = connection.catalog.get_table_names(extract_table.table_name.schema_name)
            logger.debug(f"  Tables available in {path_to_database} are: {table_names}")

            row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {extract_table.table_name}")
            logger.info(f"  The number of rows in table {extract_table.table_name} is {row_count}.")

        logger.debug("  The connection to the Hyper file has been closed.")
    logger.debug("  The Hyper process has been shut down.")

def publishToTableauServer(username,password,project,tabUrl,unique_filename,mode):
    tableau_auth = TSC.TableauAuth(username,password)
    server = TSC.Server(tabUrl)
    server.add_http_options({'verify': CA})
    
    project_id=''
    file_path = unique_filename

    with server.auth.sign_in(tableau_auth): 
        all_project_items, pagination_item = server.projects.get()
        for proj in all_project_items:
            if proj.name == project:
                project_id = proj.id
                new_datasource = TSC.DatasourceItem(project_id)
                try:
                  new_datasource = server.datasources.publish(new_datasource, file_path, mode)
                  logger.debug('  DataSource Published')
                except ServerResponseError as e:
                  server.auth.sign_out()
                  logger.error('  DataSource failed to publish ')
                  logger.error(e)
                  raise Exception(e)
                  sys.exit(1)

    server.auth.sign_out()

    if os.path.exists(unique_filename):
        os.remove(unique_filename)
    else:
        logger.warning("The file does not exist")