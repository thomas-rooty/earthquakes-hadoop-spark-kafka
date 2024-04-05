#!/bin/bash

HDFS_DEST_DIR="/sismique/data/"

# Dossier
hadoop fs -test -d $HDFS_DEST_DIR
if [ $? -ne 0 ]; then
  echo "Le répertoire $HDFS_DEST_DIR n'existe pas dans HDFS. Création..."
  hadoop fs -mkdir -p $HDFS_DEST_DIR
fi

# Fichiers
for file in /volume_namenode/*.csv; do
  echo "Importation de $file dans HDFS..."
  hadoop fs -put $file $HDFS_DEST_DIR/
done

echo "Importation terminée."
