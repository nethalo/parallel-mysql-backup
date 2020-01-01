#!/bin/bash
#
# Distributed Backup data using MySQL mysqldump tool
# Daniel Guzman Burgos <daniel.guzman.burgos@percona.com>
#

clear

set -o pipefail

lockFile="/var/lock/distBackup.lock"
errorFile="/var/log/distBackup.err"
logFile="/var/log/distBackup.log"
mysqlUser=percona
mysqlPort=3306
remoteHost=localhost
backupPath="/data/backups/$(date +%Y%m%d)/"
email="daniel.guzman.burgos@percona.com"

schemaName="sb"

function sendAlert () {
        if [ -e "$errorFile" ]
        then
                alertMsg=$(cat $errorFile)
                echo -e "${alertMsg}" | mailx -s "[$HOSTNAME] ALERT Parallel backup"
        fi
}

function destructor () {
        #sendAlert
        rm -f "$lockFile" "$errorFile" unlockStartSlaves
}

# Setting TRAP in order to capture SIG and cleanup things
trap destructor EXIT INT TERM

function verifyExecution () {
        local exitCode="$1"
        local mustDie=${3-:"false"}
        if [ $exitCode -ne "0" ]
        then
                msg="[ERROR] Failed execution. ${2}"
                echo "$msg" >> ${errorFile}
                if [ "$mustDie" == "true" ]; then
                        exit 1
                else
                        return 1
                fi
        fi
        return 0
}

function setLockFile () {
        if [ -e "$lockFile" ]; then
                trap - EXIT INT TERM
                verifyExecution "1" "Script already running. $lockFile exists"
                sendAlert
                rm -f "$errorFile"
                exit 2
        else
                touch "$lockFile"
        fi
}

function logInfo (){
        echo "[$(date +%y%m%d-%H:%M:%S)] $1" >> $logFile
}

function verifyMysqldump () {
        which mysqldump &> /dev/null
        verifyExecution "$?" "Cannot find mysqldump tool" true
        logInfo "[OK] Found 'mysqldump' bin"
}

function verifyMysql () {
        which mysql &> /dev/null
        verifyExecution "$?" "Cannot find mysql client" true
        logInfo "[OK] Found 'mysql' bin"
}

function listTables () {
        out=$(mysql -u$mysqlUser  -h${remoteHost} -N -e"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schemaName}'" 2>&1)
        verifyExecution "$?" "Can't get the count table list. $out" true
        logInfo "[Info] Count tables OK"
        tablesPerServer=$(mysql -u$mysqlUser  -h${remoteHost} -N -e"SELECT ceiling("${out}"/"${index}")" 2>&1)
        verifyExecution "$?" "Can't get the table list. $tablesPerServer" true
        logInfo "[Info] table list gathered"
}

function findSlaves () {
        out=$(mysql -u$mysqlUser  -h${remoteHost} -NB -e"SHOW SLAVE HOSTS" 2>&1)
        verifyExecution "$?" "Couldn't execute SHOW SLAVE HOSTS. Finishing script. $out" true
        logInfo "[Info] SHOW SLAVE HOSTS executed"
        IFS=''
        slaveslist=$(echo $out | awk '{print $2}')
        index=0
        unset IFS
        for i in $(echo $slaveslist);
        do
                slaves[$index]=$i;
                index=$(($index+1))
        done
}

makeDistLists () {
        out=$(mysql -u$mysqlUser  -h${remoteHost} -NB -e"CREATE DATABASE IF NOT EXISTS percona" 2>&1)
        verifyExecution "$?" "Couldn't execute CREATE DATABASE IF NOT EXISTS percona. Finishing script. $out" true
        logInfo "[Info] CREATE DATABASE IF NOT EXISTS percona"

        out=$(mysql -u$mysqlUser  -h${remoteHost} -NB -e"create table if not exists percona.metabackups (id int(11) unsigned not null auto_increment, host varchar(255), chunkstart int(11) unsigned not null, primary key (id), key host (host)) engine=innodb;" 2>&1)
        verifyExecution "$?" "Couldn't execute CREATE TABLE IF NOT EXISTS percona.metabackups. Finishing script. $out" true
        logInfo "[Info] CREATE TABLE IF NOT EXISTS percona.metabackups"

        out=$(mysql -u$mysqlUser  -h${remoteHost} -NB -e"TRUNCATE TABLE percona.metabackups" 2>&1)
        verifyExecution "$?" "Couldn't execute TRUNCATE TABLE percona.metabackups. Finishing script. $out" true
        logInfo "[Info] TRUNCATE TABLE percona.metabackups"

        multiplier=0
        indexminusone=$(($index-1))
        for i in $(seq 0 ${indexminusone});
        do
                out=$(mysql -u$mysqlUser  -h${remoteHost} -NB -e"INSERT INTO percona.metabackups (host,chunkstart) VALUES('${slaves[$i]}',$(($tablesPerServer*$multiplier)))" 2>&1)
                verifyExecution "$?" "Couldn't execute INSERT INTO percona.metabackups (host,chunkstart) VALUES('${slaves[$i]}',$(($tablesPerServer*$multiplier))) . $out" true
                logInfo "[Info] Executed INSERT INTO percona.metabackups (host,chunkstart) VALUES('${slaves[$i]}',$(($tablesPerServer*$multiplier)))"

                multiplier=$(($multiplier+1))
        done
}

freezeServers () {
        out=$(mysql -u$mysqlUser  -h${remoteHost} -N -e"lock binlog for backup" 2>&1)
        verifyExecution "$?" "Cannot set lock binlog for backup. $out" true
        logInfo "[Info] lock binlog for backup set"
}

findMostUpdatedSlave () {
        indexminusone=$(($index-1))
        for i in $(seq 0 ${indexminusone});
        do
                host=${slaves[$i]};
                out=$(mysql -u$mysqlUser  -h${host} -e"SHOW SLAVE STATUS\G " | grep -i "exec_master_log_pos" | awk -F": " '{print $2}' 2>&1)
                verifyExecution "$?" "Cannot get slave status position on $host. $out" true
                logInfo "[Info] slave status position on $host"
                executedPos[$i]=$out;

                out=$(mysql -u$mysqlUser  -h${host} -e"SHOW SLAVE STATUS\G " | grep "Relay_Master_Log_File" | awk -F": " '{print $2}' 2>&1)
                verifyExecution "$?" "Cannot get slave status file on $host. $out" true
                logInfo "[Info] slave status file on $host"
                executedFile[$i]=$out;
        done

        IFS=$'\n' sorted=($(sort <<<"${executedPos[*]}"))
        unset IFS
        greatestPos=$(echo ${sorted[-1]})

        IFS=$'\n' sorted=($(sort <<<"${executedFile[*]}"))
        unset IFS
        greatestFile=$(echo ${sorted[-1]})
}

syncSlaves () {

        indexminusone=$(($index-1))
        for i in $(seq 0 ${indexminusone});
        do
                host=${slaves[$i]};
                out=$(mysql -u$mysqlUser  -h${host} -e"STOP SLAVE; START SLAVE UNTIL MASTER_LOG_FILE = '${greatestFile}', MASTER_LOG_POS = ${greatestPos}" 2>&1)
                verifyExecution "$?" "Cannot STOP SLAVE; START SLAVE UNTIL MASTER_LOG_FILE = '${greatestFile}', MASTER_LOG_POS = ${greatestPos} on $host. $out" true
                logInfo "[Info] set STOP SLAVE; START SLAVE UNTIL MASTER_LOG_FILE = '${greatestFile}', MASTER_LOG_POS = ${greatestPos} on $host"
        done
}

startDump () {
        out=$(mkdir -p $backupPath 2>&1)
        verifyExecution "$?" "Can't create $backupPath directory ${out}" true
        logInfo "[Info] Created $backupPath directory"

        indexminusone=$(($index-1))
        for i in $(seq 0 ${indexminusone});
        do
                host=${slaves[$i]};
                lowerChunk=$(mysql -u$mysqlUser  -h${remoteHost} -N -e"select chunkstart from percona.metabackups where host like '${host}'" 2>&1)
                verifyExecution "$?" "Can't get limit for host ${host} on percona.metabackups table. $limit" true
                logInfo "[Info] Limit chunk OK"
                tables=$(mysql -u$mysqlUser  -h${remoteHost} -N -e"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schemaName}' LIMIT $lowerChunk,$tablesPerServer" 2>&1)
                verifyExecution "$?" "Can't get the table list for host ${host}. $tables" true
                logInfo "[Info] Tables list for $host OK"

                out=$(mysqldump -u${mysqlUser}  -h${host} --single-transaction --lock-for-backup ${schemaName} $tables > $backupPath/${host}.sql 3>&1 &)
                verifyExecution "$?" "Problems dumping $host. $out"
                logInfo "[OK] Dumping $host"
        done
}

unlockStartSlaves () {
        out=$(mysql -u$mysqlUser  -h${remoteHost} -N -e"UNLOCK BINLOG" 2>&1)
        verifyExecution "$?" "Cannot unlock binlog. $out" true
        logInfo "[Info] UNLOCK BINLOG executed"

        indexminusone=$(($index-1))
        for i in $(seq 0 ${indexminusone});
        do
                host=${slaves[$i]};
                out=$(mysql -u$mysqlUser  -h${host} -e"START SLAVE" 2>&1)
                verifyExecution "$?" "Cannot set start slave on $host. $out" true
                logInfo "[Info] set start slave on $host"
        done
}

verifyMysql
findSlaves
listTables
makeDistLists
freezeServers
findMostUpdatedSlave
syncSlaves
startDump
unlockStartSlaves
# wait until all mysqldump instances finish.
