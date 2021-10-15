#
# Registers and approves mediawiki oauth consumer for test purposes
# Works against mw 1.36, maybe others.

from lxml import etree
import random
import requests
import re
import string
from urllib.parse import urljoin


BASE_URL = 'http://localhost:8083/index.php'
CALLBACK_URL = 'http://localhost:8080/oauth/oauth_verify', # TODO: inject
USERNAME = 'Admin'
PASSWORD = 'adminpassword'


def get_title(session, title):
    return session.get(BASE_URL, params={'title': title})


def form_values(form):
    params = {}
    for elem in form.xpath('.//input'):
        name = elem.get('name')
        value = elem.get('value')
        if value is not None:
            params[name] = value
    return params

def post_form(session, action, params, **kwargs):
    return session.post(urljoin(BASE_URL, action), params=params, **kwargs)


def random_string(length):
    # https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


# OAuth doesn't have api's for registration, whee!
def login(session):
    res = get_title(session, 'Special:UserLogin')
    res.raise_for_status()
    root = etree.fromstring(res.text)
    # TODO: probably: //form[@name=userlogin]
    form = next(form for form in root.xpath('//form') if form.get('name') == 'userlogin')
    params = form_values(form)
    params['wpName'] = USERNAME
    params['wpPassword'] = PASSWORD

    res = post_form(session, form.get('action'), params, allow_redirects=False)
    if res.status_code != 302:
        raise Exception('Failed login')


def propose_consumer(session):
    title = 'Special:OAuthConsumerRegistration/propose'
    res = get_title(session, title)
    res.raise_for_status()
    root = etree.fromstring(res.text)
    form = next(form for form in root.xpath('//form') if 'OAuthConsumerRegistration' in form.get('action'))
    default_params = form_values(form)
    res = post_form(session, form.get('action'), {
        # Only one consumer of the same name should exist. We should only have one, but
        # make testing easier by having all names unique-ish.
        'wpname':'mw-oauth-proxy: ' + random_string(6),
        'wpversion': '1.0', # consume version
        'wpoauthVersion': '1', # protocol version
        'wpdescription': '...',
        'wpcallbackUrl': 'http://localhost:8080/oauth/oauth_verify', # TODO: inject
        'wpemail':  default_params['wpemail'],
        'wpwiki': '*',
        'wpgranttype': 'authonly',
        'wprestrictions': '0.0.0.0/0\n::/0',
        'wprsaKey': '',
        'wpagreement': '1',
        'wpEditToken': default_params['wpEditToken'],
        'wpaction': default_params['wpaction'],
        'title': title,
    })

    res.raise_for_status()

    # There isn't a great programatic way to say if it worked, just try and
    # extract the consumer key and secret. If it fails, it didn't work. Hope
    # noone changes the i18n keys...
    # TODO: Are lengths specified in the spec? We have a terminator so
    # shouldn't be important.
    match = re.search(
        r'You have been assigned a consumer token of <b>([a-z0-9]+)</b> '
        r'and a secret token of <b>([a-z0-9]+)</b>', res.text)
    token, secret = match.groups()
    return {'token': token, 'secret': secret}


def approve_consumer(session, url):
    res = session.get(url)
    res.raise_for_status()
    root = etree.fromstring(res.text)
    form = next(form for form in root.xpath('//form') if 'OAuthManageConsumers' in form.get('action'))
    params = form_values(form)
    res = post_form(session, form.get('action'), {
        'wpaction': 'approve',
        'wpreason': '...',
        'wpEditToken': params['wpEditToken'],
        'wpconsumerKey': params['wpconsumerKey'],
        'wpchangeToken': params['wpchangeToken']
    })
    res.raise_for_status()


def approve_any_consumers(session):
    res = get_title(session, 'Special:OAuthManageConsumers/proposed')
    res.raise_for_status()
    root = etree.fromstring(res.text)
    for link in root.xpath('//li[@class="mw-mwoauthmanageconsumers-proposed"]/strong/a'):
        approve_consumer(session, urljoin(BASE_URL, link.attrib['href']))




session = requests.Session()
login(session)
credentials = propose_consumer(session)
approve_any_consumers(session)

print("""
OAUTH_CONSUMER_KEY={token}
OAUTH_CONSUMER_SECRET={secret}
""".format(**credentials))
