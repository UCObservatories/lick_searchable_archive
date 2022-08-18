#!/usr/bin/bash

bkup_dir=/pg_data/backups/
file=$bkup_dir/archive_db_`date --utc +%Y%m%d_%H%M`.dump.gz
if pg_dump  -U postgres archive --no-owner --no-comments | gzip > $file ; then
    if [[ -s $file ]]; then
        find $bkup_dir -mtime +7 -type f -delete
    fi
fi


