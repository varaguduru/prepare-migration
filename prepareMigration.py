#!/usr/bin/python

import csv
import codecs
import json
import urllib3 
import os
import socket
import sys
import logging as log
from logging.handlers import RotatingFileHandler
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager(cert_reqs='CERT_NONE')

logFormatter = log.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
rootLogger = log.getLogger()

fileHandler = RotatingFileHandler("prepare-script.log", mode='a', maxBytes=1024*1024, 
                                 backupCount=2, encoding=None, delay=0)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = log.StreamHandler()
consolelogfmt = log.Formatter("[%(levelname)-5.5s]  %(message)s")
consoleHandler.setFormatter(consolelogfmt)
consoleHandler.setLevel(log.ERROR)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(log.INFO)

def readConfig():
    try:
        with open(".config.json") as R:
             config = json.loads(R.read())
    except Exception as e:
        print("\nERROR reading config file. Reason: {}".format(e))
        log.error("ERROR reading config file. Reason: {}".format(e))
    
    return config

def getCreds(groupname, username, apiinfo):

    userCreds = {}
    adminendpoint = str(apiinfo['adminendpoint']['adminurl'])
    adminpass = str(apiinfo['adminendpoint']['adminpass'])
    auth = "sysadmin:{}".format(adminpass)
    headers = urllib3.make_headers(basic_auth=auth)

    url = '{}/user?userId={}&groupId={}'.format(adminendpoint, username, groupname)
    try:
        log.info("Quering admin API to get cannonical user ID for user {}".format(username))
        response = http.request('GET', url, headers=headers)
        if response.status==200:
            log.info("Got cannonical user ID for user {}".format(username))
            result = json.loads(response.data)
            userCreds['userid'] = str(result['canonicalUserId'])
        elif response.status==204:
            log.error("User/Group {}/{} seems to be not exist".format(username, groupname))
            exit(1)
        elif response.status==200 and not str(result['active']):
            log.error("User {} seems to be not active".format(username))
            exit(1)
    except Exception as e:
        log.error("Failed to get cannonical user ID for user {}/{}. ENDPOINT: {}.".format(groupname, username, adminendpoint))
        log.error("ERROR msg: {}. HTTP Status: {}.".format(e, response.status))
        exit(1)

    url = '{}/user/credentials/list/active?userId={}&groupId={}'.format(adminendpoint, username, groupname)

    try:
        log.info("Quering admin API to get active S3 credentials for user {}".format(username))
        response = http.request('GET', url, headers=headers)
        if response.status==200:
            result = json.loads(response.data)[0]
            userCreds['accesskey'] = str(str(result['accessKey']))
            userCreds['secretkey'] = str(str(result['secretKey']))
            log.info("Got the s3 Creds for user {}".format(username))
        elif response.status == 204:
            log.error("There seem to be no active S3 credential for user {}. atleast API didn't return anything.".format(username))
            log.warn("Checking with User if new keys to be created")
            answer = raw_input("There seems to be no active S3 keys found. Would like to create one?(Y/N): ")
            if answer.lower() == 'y' or answer.lower() == 'yes':
                log.info("Creating new S3 creds as confirmed by User")
                url = '{}/user/credentials?userId={}&groupId={}'.format(adminendpoint, username, groupname)
                response = http.request('PUT', url, headers=headers)
                if response.status == 200:
                    result = json.loads(response.data)
                    log.info("New S3 creds created for User {}. AccessKey: {}".format(username, str(str(result['accessKey'])) ))
            else:
                log.error("Exiting as User do not want to create new S3 creds")
                log.error("Failed to get active S3 credentials for user {}.".format(username))
                exit(1)

    except Exception as e:
        log.error("Failed to get active S3 credentials for user {}. Error:{}".format(username, e))
        exit(1)
    
    return userCreds

def isOpen(ip,port):
   
   s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   try:
    s.connect((ip, int(port)))
    s.shutdown(2)
    log.info("End point reachable {}:{}.".format(ip, port))
    return True
   except:
    log.error("End point is not reachable {}:{}.".format(ip, port))
    return False
    

def getCsvData(csvFile, config):

    csvDict = {}

    log.info("Reading the CSV file {}".format(csvFile))
    
    with open(csvFile) as csvfile:
        bucketInfo = {}
        try:
            reader = csv.DictReader(codecs.EncodedFile(csvfile, 'utf-8-sig', 'utf-8'))
        except Exception as e:
             log.error("Issue while reading the CSV: {}".format(e))
             exit(1)

        for row in reader:
            for key in row.keys():
                try:
                    if row[key].decode('ascii', 'ignore').encode('utf-8').strip() != '':
                        if 'source' in key.lower() and 'endpoint' in key.lower():
                            config['source']['s3endpoint'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                            log.info("Checking if the source s3 end point reachable {}:443.".format(config['source']['s3endpoint']))
                            isOpen(config['source']['s3endpoint'], "443")
                            sregion = config['source']['s3endpoint'].split('.')[0].split('-')[1]
                            config['source']['adminendpoint'] = config['source']['region'][sregion]
                            log.info("Checking if the source admin end point reachable {}:19443.".format(config['source']['adminendpoint']['adminurl']))
                            isOpen(config['source']['adminendpoint']['adminurl'].split(':')[1].replace('/', ''), "19443")
                        elif 'source' in key.lower() and 'bucket' in key.lower():
                            bucketInfo['source_bucket'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                        elif 'source' in key.lower() and 'user' in key.lower():
                            bucketInfo['source_user'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                        elif 'source' in key.lower() and 'group' in key.lower():
                            bucketInfo['source_group'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                        elif 'target' in key.lower() and 'endpoint' in key.lower():
                            config['target']['s3endpoint'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                            log.info("Checking if the target s3 end point reachable {}:443.".format(config['target']['s3endpoint']))
                            isOpen(config['target']['s3endpoint'], "443")
                            tregion = config['target']['s3endpoint'].split('.')[0].split('-')[1]
                            config['target']['adminendpoint'] = config['target']['region'][tregion]
                            log.info("Checking if the target admin end point reachable {}:19443.".format(config['target']['adminendpoint']['adminurl']))
                            isOpen(config['target']['adminendpoint']['adminurl'].split(':')[1].replace('/', ''), "19443")
                        elif 'target' in key.lower() and 'bucket' in key.lower():
                            bucketInfo['target_bucket'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                        elif 'target' in key.lower() and 'user' in key.lower():
                            bucketInfo['target_user'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                        elif 'target' in key.lower() and 'group' in key.lower():
                            bucketInfo['target_group'] = row[key].decode('ascii', 'ignore').encode('utf-8').strip()
                except Exception as e:
                     log.error("CSV column names or config seems to missing source/target keywords. Please verify")
                     log.error("ERROR: {}".format(e))
                     exit(1)
            log.info("Collecting the details of *** BUCKET: {} ***".format(bucketInfo['source_bucket']))
            log.info("Connecting to source admin service for Credentials of user {}".format(bucketInfo['source_user']))
            screds = getCreds(bucketInfo['source_group'], bucketInfo['source_user'], config['source'])
            bucketInfo['source_user_id'] = screds['userid']
            bucketInfo['source_accesskey'] = screds['accesskey']
            bucketInfo['source_secretkey'] = screds['secretkey']
            log.info("Connecting to Target admin service for Credentials of user {}".format(bucketInfo['target_user']))
            tcreds = getCreds(bucketInfo['target_group'], bucketInfo['target_user'], config['target'])
            bucketInfo['target_user_id'] = tcreds['userid']
            bucketInfo['target_accesskey'] = tcreds['accesskey']
            bucketInfo['target_secretkey'] = tcreds['secretkey']
            log.info("Genarating Properties file for {}".format(bucketInfo['source_bucket']))
            with open("migration.properties.{}".format(bucketInfo['source_bucket']), 'w') as outfile:
                 outfile.write("source.bucket={}\n".format(bucketInfo['source_bucket']))
                 outfile.write("source.endpoint=https://{}\n".format(config['source']['s3endpoint']))
                 outfile.write("source.region={}\n".format(sregion))
                 outfile.write("source.provider={}\n".format(config['source']['provider']))
                 outfile.write("source.access.key={}\n".format(bucketInfo['source_accesskey']))
                 outfile.write("source.secret.key={}\n".format(bucketInfo['source_secretkey']))
                 outfile.write("source.user.id={}\n".format(bucketInfo['source_user_id']))
                 outfile.write("\n")
                 outfile.write("target.bucket={}\n".format(bucketInfo['target_bucket']))
                 outfile.write("target.endpoint=https://{}\n".format(config['target']['s3endpoint']))
                 outfile.write("target.region={}\n".format(tregion))
                 outfile.write("target.provider={}\n".format(config['target']['provider']))
                 outfile.write("target.access.key={}\n".format(bucketInfo['target_accesskey']))
                 outfile.write("target.secret.key={}\n".format(bucketInfo['target_secretkey']))
                 outfile.write("target.user.id={}\n".format(bucketInfo['target_user_id']))
                 outfile.write("\n")
                 outfile.write("target.sourceUser.map={}|{}".format(bucketInfo['source_user_id'],bucketInfo['target_user_id']))
                 outfile.write("\n")
                 outfile.write("client.timeout={}".format(config['connectiontimeout']))
                 outfile.write("\n")
            if os.path.isfile("migration.properties.{}".format(bucketInfo['source_bucket'])):
                log.info("Properties file created for {}-- > migration.properties.{}".format(bucketInfo['source_bucket'], bucketInfo['source_bucket']))
            else:
                log.error("Failed to create the migration.properties.{}".format(bucketInfo['source_bucket']))
                exit(1)
                 

def main():

    if len(sys.argv) > 1:
        log.info("Got the {} file as input from the commandline".format(sys.argv[1]))
        csvFile = sys.argv[1]
        if csvFile.split('.')[-1].lower() == 'csv' and os.path.isfile(csvFile):
            log.info("Found th CSV file {} on current working directory".format(csvFile))
        else:
            log.error("Not in the CSV format or Not Found th CSV file {}".format(csvFile))
            exit(1)
    else:
        log.error("Please make sure you provide CSV file as cmd line argument")
        exit(1)


    log.info("Reading the config file")
    config = readConfig()
    
    if any(sourceItem in config['source'].keys() for sourceItem in ["region", "provider"]):
         log.info("Source details: {}".format(config['source']))
    elif any(targetItem in config['target'].keys() for targetItem in ["region", "provider"]):
         log.info("Source details: {}".format(config['target']))
    else:
         log.error("Looks like one of the source/target keywrods are missing. Please verify")
         exit(1)
    
    getCsvData(csvFile, config)

if __name__ == "__main__":
    main()
