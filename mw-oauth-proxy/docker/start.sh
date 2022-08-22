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

ensure_project_target() {
    project=$1
    target=$2
    if [ ! -f "${target}" -o "${REBUILD_OAUTH_WAR}" = "yes" ]; then
        echo "Triggering ${project} build"
        echo ${target}
        pushd "${git_root}"
        sleep 5  # Why?
        ./mvnw -pl "${project}" clean verify
        popd
    fi
    if [ ! -f "${target}" ]; then
        echo "Couldn't locate ${project} target at: ${target}"
        exit 1
    fi
}

RDF_PROJECT_VERSION="$(cd "${git_root}"; ./mvnw -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)"

MW_OAUTH_WAR="${MW_OAUTH_WAR:-../target/mw-oauth-proxy-${RDF_PROJECT_VERSION}.war}"
ensure_project_target mw-oauth-proxy "${MW_OAUTH_WAR}"

LOGGING_JAR="${LOGGING_JAR:-../../jetty-logging/target/jetty-logging-${RDF_PROJECT_VERSION}-jar-with-dependencies.jar}"
ensure_project_target jetty-logging "${LOGGING_JAR}"

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
rm -f jetty/lib/jetty-logging-*-jar-with-dependencies.jar
cp "${LOGGING_JAR}" jetty/lib/


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
