hdfs dfs -rm -f -R -skipTrash {{ hdfs_location }}/*
hdfs dfs -put {{ stage_dir }}/{{ file_mask }} {{ hdfs_location }}
