#!/bin/bash
set -e
. /usr/local/bin/cronUtils.sh wcqs

function log() {
  echo "$(date --iso-8601=seconds) $1"
}

DUMP_FILENAME=latest-mediainfo.ttl.gz
MUNGED_DIR=${DATA_DIR}/munged
mkdir -p "${MUNGED_DIR}"
ENDPOINT="http://localhost:9999"

# do not delete the data for now - see https://phabricator.wikimedia.org/T251515#6393397
# trap 'rm -rf ${DATA_DIR}/${DUMP_FILENAME} ${DATA_DIR}/munged' EXIT

log "Download latest dump"
curl -o ${DATA_DIR}/${DUMP_FILENAME} \
https://dumps.wikimedia.your.org/other/wikibase/commonswiki/${DUMP_FILENAME}

log "Running munge script"
${DEPLOY_DIR}/munge.sh \
-f ${DATA_DIR}/${DUMP_FILENAME} \
-c 50000 \
-d ${DATA_DIR}/munged \
-- \
--wikibaseHost commons.wikimedia.org \
--conceptUri http://www.wikidata.org \
--commonsUri https://commons.wikimedia.org \
--skolemize

# added for debugging purposes - see https://phabricator.wikimedia.org/T251515#6393397
log "Munged dir - ${MUNGED_DIR}"

NEW_NAMESPACE="wcqs${today}"
${DEPLOY_DIR}/createNamespace.sh $NEW_NAMESPACE $ENDPOINT /etc/wcqs/RWStore.wcqs.properties wcq || exit 1

log "Loading data"
${DEPLOY_DIR}/loadData.sh -n $NEW_NAMESPACE -h $ENDPOINT -d "${MUNGED_DIR}"

log "Data reload complete, switching active namespace"
replaceNamespace wcq $NEW_NAMESPACE $ENDPOINT
log "All done"


