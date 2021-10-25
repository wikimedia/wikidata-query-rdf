FROM mediawiki:latest

COPY LocalSettings.local.php /var/www/html/
COPY composer.local.json /var/www/html/

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
    echo 'include "LocalSettings.local.php";' >> /var/www/html/LocalSettings.php && \
    apt update && apt -y install python3-lxml python3-requests wget && \
    wget -O /tmp/composer.phar https://getcomposer.org/download/2.1.9/composer.phar && \
    (echo '4d00b70e146c17d663ad2f9a21ebb4c9d52b021b1ac15f648b4d371c04d648ba  /tmp/composer.phar' | sha256sum -c -) && \
    php /tmp/composer.phar update --no-dev --working-dir=/var/www/html && \
    rm /tmp/composer.phar

