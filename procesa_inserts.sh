#!/bin/bash

###################
### ARGUMENTOS  ###
###################
DIR2PROC=$1

####################
### CONSTATNES  ####
####################

LOGFILE=/home/operaciones/procesa_inserts.log
DATE_NOW=`date +%Y%m%d-%H%M%S`
MYSQL_HOST="kpis"
MYSQL_USER="kpis"
MYSQL_PWD="push"
MYSQL_DATABASE="kpisdb"

#################
### FUNCIONES ###
#################

log()
{
    message="$@"
    echo "{`date`} $message"
    echo "{`date`} $message" >>$LOGFILE
}

check_row()
{
    HOST=$1
    DATE=`echo "$2 $3"`
    log "[CHECK_ROW] Comprobando si existe fila el: $DATE para el host: $HOST tipo: $4"
    RESULT=`mysql -s -h kpis -u kpis -ppush kpisdb -e "select fecha from kpisdb.sistema where host = $HOST and fecha = $DATE and tipo = $4  ORDER BY fecha DESC LIMIT 1" -N`
    if [ "$?" -eq 0 ]; then
        if [ -n "$RESULT" ]; then
        #### ROW NOT EMPTY
            log "[CHECK_ROW] KPI insertado el: $DATE para el host $HOST tipo: $4"
            CHK_ROW=0
        else 
        #### ROW EMPTY
            log "[CHECK_ROW] KPI NO insertado el: $DATE para el host $HOST tipo: $4"
            CHK_ROW=1
        fi
    else
         log "[CHECK_ROW] ERROR conectando con $MYSQL_HOST a la base de datos: $MYSQL_DATABASE"
         exit 1
    fi
    #items=$(echo $result | tr " " "\n")
 
    #for item in $items
    #do
    #    echo "$item"
    #done 
}

dump_data_to_mysql()
{
    file_inserts="$1"
    inserciones=`cat "$file_inserts"|wc -l `
    log "[DUMP_DATA] Lineas a insertar:$inserciones"
    mysql --host=kpis --user=$MYSQL_USER --password=$MYSQL_PWD $MYSQL_DATABASE -e "show tables" >/dev/null
    mysql_OK=$?
    log "[DUMP_DATA] Valor de ok:$mysql_OK"
    if [ "$mysql_OK" -eq 0 ]; then
            log "status mysql Ok"
                    if [ -f $file_inserts ]; then
                        mysql --host=$MYSQL_HOST --user=$MYSQL_USER --password=$MYSQL_PWD $MYSQL_DATABASE <$file_inserts >> sql.log 2>&1
                        if [ $? -ne 0 ]; then 
                            log "[DUMP_DATA] Error en la inserccion de KPIs. REVISE fichero sql.log. Tratando de ejecutar $file_inserts \
                                 Contacte con el adminsitrador del sistema. Ficheros almacenados en /home/operaciones/kpis-procesados/$DATE_NOW"
                            fin 1
                        fi
                        log "[DUMP_DATA] Fin insertar"
                    else
                        log "[DUMP_DATA] No hay datos que insertar"
                fi
    else
        log "[DUMP_DATA] ERROR conectando con $MYSQL_HOST a la base de datos: $MYSQL_DATABASE"
    fi
}

fin(){
    if [ -f /tmp/$DATE_NOW.sql ]; then
        mv /tmp/$DATE_NOW.sql /home/operaciones/kpis-procesados/$DATE_NOW/
    fi
    #rm /temp/$DATE_NOW.sql
    if [ $1 -eq 0 ]; then
        gzip -r /home/operaciones/kpis-procesados/$DATE_NOW/
        rm sql.log
        exit 0
    else 
        mv sql.log /home/operaciones/kpis-procesados/$DATE_NOW/
        exit 1
    fi
}

flush_stdin(){
    while read -r -t 0; do read -r; done
}
    
#############
### MAIN  ###
#############

mkdir -p /home/operaciones/kpis-procesados/$DATE_NOW
FILES_PROC=`ls -l $1/*.txt | wc -l`
log "Se van a procesar $FILES_PROC ficheros"

for file in $1/*.txt; do
    #echo $file >> dev.log
    log ".-.-.-.-Inicio proceso inserts.-.-.-.-.-.-.-.-.-.-"
    FILE_PROC=`echo $file | sed 's/\// /' | awk {'print $2'}`
    log "Procesando: $FILE_PROC"
    while read line 
    do
        COLUMN1=`echo $line | awk {'print $5'}`
        COLUMN2=`echo $line | awk {'print $6'}`
        COLUMNS=`echo $COLUMN1 $COLUMN2`
        HOST=`echo $COLUMNS | sed 's/(//' | awk -F ',' {'print $1'}`
        DATE=`echo $COLUMNS | awk -F ',' {'print $2'}`
        TIPO=`echo $COLUMNS | awk -F ',' {'print $3'}`
        VALOR=`echo $COLUMNS | sed 's/)//' |sed 's/;//' | awk -F ',' {'print $4'}`
	check_row $HOST $DATE $TIPO 
	if [ $CHK_ROW == 1 ]; then
            ####
            log "inertar kpi: $line " 
            echo $line >> /tmp/$DATE_NOW.sql
        fi
    done < $file
    log "moviendo fichero $FILE_PROC a kpis-procesados/$DATE_NOW"
    mv $file /home/operaciones/kpis-procesados/$DATE_NOW/$FILE_PROC
done
    flush_stdin
    read -ep "Presione 0 para insertar en la BBDD: " ans
    if [ $ans -eq 0 ]; then
         dump_data_to_mysql "/tmp/$DATE_NOW.sql"
    fi
fin 0
