#!/usr/bin/python3
#
# Simple tool to manually ship reconciliation events to the flink pipeline.
# These events are being generated on an hourly basis the UpdaterReconcile spark
# application. But for testing or solving some particular inconsistencies reported
# by users this script can be used.
# Process is simple, it takes a set of items to reconcile, ask mediawiki what is
# the latest revision # and ship an event to the rdf-streaming-updater.reconcile
# topic with the source tag provided.
#
# Examples:
# - Reconcile items from wikidata for wdqs@codfw:
#   python3 reconcile_items.py --domain_project www.wikidata.org \
#               --mw_api_endpoint https://api-ro.discovery.wmnet \
#               --reconcile_source wdqs_sideoutputs_reconcile@codfw \
#               Q1 Q2 Q3 P3
#
# - Reconcile items from commons for wcqs@eqiad
#   python3 reconcile_items.py --domain_project commons.wikimedia.org \
#               --mw_api_endpoint https://api-ro.discovery.wmnet \
#               --reconcile_source wcqs_sideoutputs_reconcile@codfw \
#               M1 M2 M3

import re
import uuid
import requests

from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Optional, Dict, List, Callable

MW_API_ENDPOINT = "https://api-ro.wikimedia.org/"
EVENT_GATE_ENDPOINT = "https://eventgate-main.discovery.wmnet:4492/v1/events"
MEDIAINFO_ITEM_PREFIX = 'M'
DEFAULT_NS_MAP = 'Q=,P=Property,L=Lexeme,M=File'


class WBRepo:
    def __init__(self, hostname: str, endpoint: str, ns_to_entity_map: Dict[str, str]):
        self.hostname = hostname
        self.endpoint = endpoint
        self._session = requests.session()
        self._session.headers.update({"host": hostname})
        self.api_endpoint = f"{endpoint}/w/api.php"
        self._ns_to_entity_map = ns_to_entity_map

    def _fetch(self, extra_params: Dict[str, str]) -> requests:
        params = {
            'format': 'json',
            'formatversion': '2',
            'prop': 'revisions',
            'rvprop': 'ids',
            'rvdir': 'older',
            'action': 'query',
        }
        params.update(extra_params)
        return self._session.get(self.api_endpoint, timeout=5, params=params)

    def to_titles(self, entity: str) -> str:
        ns_text = self._ns_to_entity_map[entity[:1]]
        if ns_text:
            return f"{ns_text}:{entity}"
        return entity

    @staticmethod
    def to_pageid(entity: str) -> int:
        return int(entity[1:])

    def fetch_latest_revision(self, items: List[str]) -> Dict[str, Optional[int]]:
        mediainfo_items = [i for i in items if i.startswith(MEDIAINFO_ITEM_PREFIX)]
        items_map = {}
        if len(mediainfo_items) > 0:
            items_map.update(self.fetch_latest_revision_for_mediainfo_items(mediainfo_items))
        wikidata_items = [i for i in items if not i.startswith(MEDIAINFO_ITEM_PREFIX)]
        if len(wikidata_items) > 0:
            items_map.update(self.fetch_latest_revision_for_entities(wikidata_items))
        return items_map

    def fetch_latest_revision_for_entities(self, items: List[str]) -> Dict[str, Optional[int]]:
        response = self._fetch({'titles': '|'.join([self.to_titles(e) for e in items])})
        response.raise_for_status()
        return self.parse_response(items,
                                   response.json(),
                                   retrieve_item=lambda x: re.sub("^[^:]+:", "", x['title']))

    def fetch_latest_revision_for_mediainfo_items(self, items: List[str]) -> Dict[str, Optional[int]]:
        response = self._fetch({'pageids': '|'.join([str(self.to_pageid(e)) for e in items])})
        response.raise_for_status()
        return self.parse_response(items,
                                   response.json(),
                                   retrieve_item=lambda x: MEDIAINFO_ITEM_PREFIX + str(x["pageid"]))

    @staticmethod
    def parse_response(entities: List[str], body: Dict, retrieve_item: Callable[[Dict], str]) -> Dict[str, Optional[int]]:
        pages = [p for p in body['query']['pages'] if 'revisions' in p]
        rev_map = {retrieve_item(p): int(p['revisions'][0]['revid']) for p in pages}
        return {e: rev_map.get(e, None) for e in entities}


class EventSender:
    def __init__(self,
                 eventgate_endpoint: str,
                 project_hostname: str,
                 reconcile_source_tag: str,
                 stream: str = "rdf-streaming-updater.reconcile"):
        self._eventgate_endpoint = eventgate_endpoint
        self._session = requests.session()
        self._project_hostname = project_hostname
        self._stream = stream
        self._reconciliation_source_tag = reconcile_source_tag

    def build_event(self, item: str, revision: int, dt: datetime):
        event_id = str(uuid.uuid4())
        request_id = str(uuid.uuid4())
        dt_iso8601 = dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat()
        return {
            "meta": {
                "dt": dt_iso8601,
                "id": event_id,
                "domain": self._project_hostname,
                "stream": self._stream,
                "request_id": request_id,
            },
            "$schema": "/rdf_streaming_updater/reconcile/1.0.0",
            "item": item,
            "revision_id": revision,
            "reconciliation_source": self._reconciliation_source_tag,
            "reconciliation_action": "CREATION",
            "original_event_info": {
                "meta": {
                    "dt": dt_iso8601,
                    "id": event_id,
                    "domain": self._project_hostname,
                    "stream": "sync_wdqs_items",
                    "request_id": request_id,
                },
                "$schema": "unused"
            }
        }

    def send(self, items: Dict[str, Optional[int]]):
        def chunks(events: list, size):
            for i in range(0, len(events), size):
                yield events[i:i+size]

        data = [self.build_event(k, v, datetime.utcnow()) for (k, v) in items.items() if v is not None]
        for chunk in chunks(data, size=50):
            self._send(chunk)

    def _send(self, events: list):
        resp = self._session.post(self._eventgate_endpoint, json=events)
        resp.raise_for_status()


def main():
    parser = ArgumentParser()
    parser.add_argument("entities", nargs='+')
    parser.add_argument("--ns_map", default=DEFAULT_NS_MAP)
    parser.add_argument("--reconcile_source", required=True)
    parser.add_argument("--domain_project", required=True)
    parser.add_argument("--eventgate_endpoint", default=EVENT_GATE_ENDPOINT)
    parser.add_argument("--mw_api_endpoint", default=MW_API_ENDPOINT)

    args = parser.parse_args()

    def parse_ns_map(ns_map: str) -> Dict[str, str]:
        return {m[0]: m[1] for m in map(lambda x: x.split("="), ns_map.split(","))}

    repo = WBRepo(hostname=args.domain_project,
                  endpoint=args.mw_api_endpoint,
                  ns_to_entity_map=parse_ns_map(args.ns_map))

    sender = EventSender(eventgate_endpoint=args.eventgate_endpoint,
                         project_hostname=args.domain_project,
                         reconcile_source_tag=args.reconcile_source)

    sender.send(repo.fetch_latest_revision(args.entities))


if __name__ == "__main__":
    main()
