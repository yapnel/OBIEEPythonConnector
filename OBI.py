from zeep import Client
from requests import Session
import requests
from zeep.transports import Transport
from zeep.settings import Settings
import xml.etree.ElementTree as ET
from zeep.wsse.username import UsernameToken
from requests.auth import HTTPProxyAuth
from pathlib import Path
import re
import logging
import os
import sys
from datetime import datetime
import ast

logger = logging.getLogger(__name__)
CA=os.path.dirname(os.path.realpath(__file__))+'/certs/CA.pem'

def createWSClient(wsdl, username, password,proxy):
    session = Session()
    session.verify = CA
    proxies = { 
               'http':proxy,
               'https':proxy
              }
    session.proxies = proxies       
    transport = Transport(session=session)
    settings = Settings(xml_huge_tree=True)

    try:
      client = Client(wsdl=wsdl, wsse=UsernameToken(username, password), transport=transport, settings=settings)
    except Exception as e:
      logger.error(e)
      sys.exit(1)
      
    return client

def login(client, username, password):
    sessionid = client.service.logon(username, password)
    return sessionid

def bindXmlViewService(client):
    return client.bind('XmlViewService')

def getSchema(client, xmlservice, reportpath, executionOptions, sessionid):
    try:
      schema = xmlservice.executeXMLQuery(report=reportpath, outputFormat="SAWRowsetSchema", executionOptions=executionOptions, sessionID=sessionid)
    except Exception as e:
      logger.error(e)
      raise Exception(e)
      sys.exit(1)
    
    if schema.rowset == None:
        client.service.logoff(sessionID=sessionid)
        raise Exception('Error getting report schema')
    
    return schema

def getColumnHeading(schema):
    columnHeading = re.findall(r'columnHeading="(.*?)"', schema.rowset)
    
    return columnHeading

def getColumnDataType(schema):
    dataTypes = re.findall(r'saw-sql:type="(.*?)"', schema.rowset)
    
    return dataTypes 



def executeXMLQuery(params,client, xmlservice, reportpath, executionOptions,sessionid):
    try:
        rp=""
        if params != None:
            rp=ast.literal_eval("{'variables':["+params+"]}")
        queryresult = xmlservice.executeXMLQuery(reportParams=rp ,report=reportpath, outputFormat="SAWRowsetData",executionOptions=executionOptions, sessionID=sessionid)
    except Exception as e:
        client.service.logoff(sessionID=sessionid)
        logger.error(e)
        raise Exception('Error executing the report')
        sys.exit(1)
    
    if queryresult.rowset == None:
        client.service.logoff(sessionID=sessionid)
        raise Exception('Error executing the report')

    return queryresult

def getQueryID(queryresult):
    queryid = queryresult.queryID

def parseQueryResult(queryresult,columnHeading,queryid,xmlservice,sessionid,dataTypes):
    records=[]
    ETobject = ET.fromstring(queryresult.rowset)
    rows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')
    
    if len(rows) == 0:
        logger.error(queryresult.rowset)
        raise Exception(queryresult.rowset)
        sys.exit(1)

    for row in rows:
        record=[]
        for index in range(0, len(columnHeading)):
            value = row.find(("{urn:schemas-microsoft-com:xml-analysis:rowset}Column" + str(index))).text
            if dataTypes[index] in ["double","numeric"]:
                if value is None:
                    record.append(value)
                else:
                    record.append(float(value))
            elif dataTypes[index] == "integer":
                if value is None:
                    record.append(value)
                else:
                    record.append(int(value))
            elif dataTypes[index] == "timestamp":
                if value is None:
                    record.append(value)
                else:
                    record.append(datetime.strptime(value, '%Y-%m-%dT%H:%M:%S'))
            elif dataTypes[index] == "date":
                if value is None:
                    record.append(value)
                else:
                    record.append(datetime.strptime(value, '%Y-%m-%d'))
            else: # default to text
                record.append(value)
        records.append(record)
        
    queryfinish = queryresult.finished
    
    while (not queryfinish):
        queryresult = xmlservice.fetchNext(queryID=queryid, sessionID=sessionid)
        ETobject = ET.fromstring(queryresult.rowset)
        rows = ETobject.findall('{urn:schemas-microsoft-com:xml-analysis:rowset}Row')
        
        for row in rows:
            record=[]
            for index in range(0, len(columnHeading)):
                value = row.find(("{urn:schemas-microsoft-com:xml-analysis:rowset}Column" + str(index))).text
                if dataTypes[index] in ["double","numeric"]:
                    if value is None:
                        record.append(value)
                    else:
                        record.append(float(value))
                elif dataTypes[index] == "integer":
                    if value is None:
                        record.append(value)
                    else:
                        record.append(int(value))
                elif dataTypes[index] == "timestamp":
                    if value is None:
                        record.append(value)
                    else:
                        record.append(datetime.strptime(value, '%Y-%m-%dT%H:%M:%S'))
                elif dataTypes[index] == "date":
                    if value is None:
                        record.append(value)
                    else:
                        record.append(datetime.strptime(value, '%Y-%m-%d'))
                else: # default to text
                    record.append(value)
            records.append(record)            
        
        # Determine if additional fetching is needed and if yes - parsing additional rows   
        queryfinish = queryresult.finished
    
    return records
