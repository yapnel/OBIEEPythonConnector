import argparse
import re
import time
import sys
import keyring
import config
from ldap3 import Server, Connection, ALL
import uuid
import logging
import logging.config
from datetime import datetime
import OBI
import Tab
import os

def main():
  
  dt=datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
  
  # Construct the parsers
  ap = argparse.ArgumentParser(description='This script is used for extracting data from OBIEE')
  subap = ap.add_subparsers(title='subcommands',description='valid subcommands',help='sub-command help',dest="sub-command",required=True)

  # Add the arguments to the main parser
  main = ap.add_argument_group('Main', 'Mandatory arguements')
  main.add_argument("-rp", "--reportpath", required=True,help="The Report Path in OBIEE Catalog")
  main.add_argument("-v", "--variable", required=False, help="Parameters for Analysis Prompts")
  main.add_argument("-l", "--log", default='INFO', choices=['INFO','DEBUG','WARNING','CRITICAL'], help=argparse.SUPPRESS)

  # Add the arguments to the Tableau subparser
  tab_parser = subap.add_parser('Tableau')  
  tab = tab_parser.add_argument_group('Tableau', 'Tableau arguements')  
  tab.add_argument("-p", "--project", required=True, help="Project name in Tableau Server to deploy the hyper data source")
  tab.add_argument("-f", "--filename", required=True, help="Tableau Data Source Filename e.g. obi.hyper")
  tab.add_argument("-m", "--mode", required=True, choices=['CreateNew','Overwrite','Append'], help="The mode used for publishing the hyper datasource")
  
  args = vars(ap.parse_args())

  loglevel=args['log']
  
  logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        "console":{
                "class": "logging.StreamHandler",
                "formatter": "standard"
        },
        "file": {
                "class": "logging.handlers.WatchedFileHandler",
                "formatter": "standard",
                "filename": os.path.dirname(os.path.realpath(__file__))+'/log/'+dt+'.log',
                "mode": "a",
                "encoding": "utf-8"
        },
    },
    'loggers': {
        '': {
            'level': getattr(logging, loglevel, None),
            'handlers': ['console','file'],
            'propagate': True
        }
    }
  })
  
  # Set the log level to CRITICAL for imported 3rd party modules
  for _ in logging.root.manager.loggerDict:
    if (_ not in ['Tab','OBI']):
      logging.getLogger(_).setLevel(logging.CRITICAL)
  
  logger = logging.getLogger('OBIExtract')

  reportpath = args['reportpath']
  params = args['variable']
  wsdl=config.wsdl
  obiusername=config.obiUsername
  obipassword=config.obiPassword

  unique_filename = args['filename']
  mode = args['mode']
  projectName= args['project']
  tabUrl=config.tabUrl
  tabusername=config.tabUsername
  tabpassword=config.tabPassword
  tableName = 'Extract'

  proxy=config.proxy
  
  logger.info('Executing OBIEE report ' + reportpath)
  
  # Extract obi data
  start_time = time.time()    
  logger.info("Step 1/10 - Creating WS Client")
  client = OBI.createWSClient(wsdl, obiusername, obipassword,proxy)
  logger.info("Step 2/10 - Login to Oracle Cloud BI Analytics")
  sessionid = OBI.login(client, obiusername, obipassword)
  logger.info("Step 3/10 - Binding XMLViewService")
  xmlservice = OBI.bindXmlViewService(client)
  logger.info("Step 4/10 - Get Schema Definition")
  schema = OBI.getSchema(client, xmlservice, reportpath, config.executionOptions, sessionid)
  logger.info("Step 5/10 - Get Column Headings")
  columnHeading = OBI.getColumnHeading(schema)
  logger.info("Step 6/10 - Get Column Datatype")
  dataTypes = OBI.getColumnDataType(schema)
  logger.info("Step 7/10 - Execute WS-SOAP")
  queryresult = OBI.executeXMLQuery(params,client,xmlservice, reportpath, config.executionOptions,sessionid)
  logger.info("Step 8/10 - Get Query ID")
  queryid = OBI.getQueryID(queryresult)
  logger.info("Step 9/10 - Parse XML Result")
  records = OBI.parseQueryResult(queryresult,columnHeading,queryid,xmlservice,sessionid,dataTypes)
  logger.info("Step 10/10 - Logout")
  client.service.logoff(sessionID=sessionid)
  logger.info("obi SOAP WS Completed Successfully --- %s seconds ---" % (time.time() - start_time))

  start_time = time.time()
  logger.info("Step 1/3 - Define Tableau Extract Table")
  dataTypes = OBI.getColumnDataType(schema)
  extract_table = Tab.createTabTable(tableName,columnHeading,dataTypes)
  logger.info("Step 2/3 - Create Hyper File")
  Tab.createHyperExtract(unique_filename,extract_table,records,tableName)
  logger.info("Step 3/3 - Publish Hyper to Tableau Server")
  Tab.publishToTableauServer(tabusername,tabpassword,projectName,tabUrl,unique_filename,mode)
  logger.info("Tableau Extract and Publication Completed Successfully --- %s seconds ---" % (time.time() - start_time))
  
if __name__== "__main__":
  main()
