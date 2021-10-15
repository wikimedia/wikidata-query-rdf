FROM mediawiki:latest

COPY LocalSettings.local.php /var/www/html/
RUN git clone --depth 1 --branch REL1_36 "https://gerrit.wikimedia.org/r/mediawiki/extensions/OAuth" /var/www/html/extensions/OAuth && \
    php /var/www/html/maintenance/install.php \
        --server http://localhost:8083 \
        --scriptpath "" \
        --dbtype sqlite \
        --dbpath /var/www/data \
        --pass adminpassword \
        --extensions OAuth \
        devwiki Admin && \
    chown -R www-data:www-data /var/www/data && \
    php /var/www/html/maintenance/update.php --quick && \
    php /var/www/html/maintenance/resetUserEmail.php --no-reset-password Admin noone@nowhere.invalid && \
    echo 'include "LocalSettings.local.php";' >> /var/www/html/LocalSettings.php &&
    apt update && apt install python3-lxml python3-requests

