# prepare-migration
This repo contains the scripts that helps the Bucket migration preperation.

# How to use the script

* Update the .config.json file with admin END point information.(refer sample config file)
* Make sure you input the CSV file to the script
* CSV file columns names must have "source endpoint", "source user", "source group", "target endpoint", "target group", "target user".
* Provide exec permissions to the script
* Run the script. 
```
#ls -latr
-rwxr-xr-x 1 root root 12713 Jul 17 21:05 prepareMigration.py
-rw-r--r--   1 root root   573 Jul 17 19:19 .config.json
# ./prepareMigration.py inputfile.csv
```

Incase of any issues, you will have log file names prepare-script.log genarated on the same folder.