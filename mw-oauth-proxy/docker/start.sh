#!/bin/bash


script_root="$(dirname $(realpath $0))"
git_root="$(git rev-parse --show-toplevel)"

export OAUTH_SUCCESS_REDIRECT

if [ "${REBUILD_EVERYTHING}" = "yes" ]; then
    REBUILD_OAUTH_WAR=yes
    docker-compose down --rmi local
fi

if [ -z "${OAUTH_CONSUMER_KEY}" ]; then
    if [ ! -z "${OAUTH_CONSUMER_SECRET}" ]; then
        echo "Both OAUTH_CONSUMER_KEY and OAUTH_CONSUMER_SECRET must be set"
        exit 1
    fi
    echo "Regitering a mediawiki consumer"
    docker-compose up -d mediawiki
    # In the future, do something less dangerous
    eval $(python3 "${script_root}/mediawiki/register-oauth.py")

    export OAUTH_CONSUMER_KEY
    export OAUTH_CONSUMER_SECRET
    export OAUTH_NICE_URL_BASE="${OAUTH_NICE_URL_BASE:-http://localhost:8083/index.php/}"
    export OAUTH_INDEX_URL="${OAUTH_INDEX_URL:-http://localhost:8083/index.php}"
fi

if [ -z "${OAUTH_NICE_URL_BASE}" ]; then
    echo "OAUTH_NICE_URL_BASE must be set"
    exit 1
fi

if [ -z "${OAUTH_INDEX_URL}" ]; then
    echo "OAUTH_INDEX_URL must be set"
    exit 1
fi


echo "Moving to $script_root"
cd $script_root

MW_OAUTH_VERSION="$(cd "${git_root}"; ./mvnw -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)"
MW_OAUTH_WAR="${MW_OAUTH_WAR:-../target/mw-oauth-proxy-${MW_OAUTH_VERSION}.war}"
if [ ! -f "${MW_OAUTH_WAR}" -o "${REBUILD_OAUTH_WAR}" = "yes" ]; then
    echo "Triggering build"
    echo ${MW_OAUTH_WAR}
    pushd "${git_root}"
    sleep 5
    ./mvnw -pl mw-oauth-proxy clean verify
    popd
fi
if [ ! -f "${MW_OAUTH_WAR}" ]; then
    echo "Couldn't locate mw-oauth-proxy WAR at: ${MW_OAUTH_WAR}"
    exit 1
fi

wait_for_cassandra() {
    echo -n "Waiting for cassandra..."
    while ! echo "show version" | docker exec -i cassandra cqlsh >/dev/null 2>&1; do
        echo -n "."
        sleep 1
    done
    echo Found
}

wait_for_port() {
    port=$1
    echo -n "Waiting for localhost:$port..."
    while ! nc -w0 localhost $1; do
        echo -n "."
        sleep 1
    done
    echo Found
}


cp "${MW_OAUTH_WAR}" jetty/webapps/ROOT.war


docker-compose up -d cassandra
wait_for_cassandra
sleep 1

echo Creating kask schema
docker-compose run cassandra cqlsh cassandra -f /etc/mediawiki-services-kask/cassandra_schema.cql

echo "Starting kask and jetty (and mediawiki, if not already)"
docker-compose up -d kask jetty mediawiki nginx

# Crazy hack, oauth needs to be accessible from the same url inside and out,
# the url is part of the signature. This makes localhost:8083 work.
docker exec -u 0:0 -d jetty socat TCP-LISTEN:8083,fork TCP:mediawiki:80

wait_for_port 8083 # mediawiki
wait_for_port 8082 # nginx
wait_for_port 8081 # kask
wait_for_port 8080 # jetty

echo "Environment started."
