@echo off
set AWS_CREDENTIAL_PROFILES_FILE=C:\tmp\credentials
mvn clean package exec:java -Dbucket=mark-borg
@pause
