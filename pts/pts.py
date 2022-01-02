#!/usr/bin/env python3
import argparse
import binascii
import dataclasses
import datetime
import json
import logging
import os
import platform
import re
import threading
import urllib.parse
from abc import ABC
from collections import Counter
from typing import Optional, Iterator, Dict, Callable, List, Type

import nsq
import requests
import requests.adapters
import requests.packages.urllib3.util.retry

import bs4

__version__ = '1.0.0'


class LinkShortener:
    SUPPORTED_SHORTENERS = ['bit.ly']

    def __init__(self, logger: logging.Logger):
        self._cache = {}
        self._logger = logger
        self._session = requests.session()
        self._session.mount('https://', CustomHTTPAdapter())
        self._session.mount('http://', CustomHTTPAdapter())

    def resolve(self, url: str) -> str:
        if url not in self._cache.keys():
            self._logger.debug(f'Resolving "{url}"...')
            response = self._session.head(url)
            response.raise_for_status()
            self._cache[url] = response.headers['location']
        return self._cache[url]

    def is_supported(self, url):
        return urllib.parse.urlparse(url).hostname in self.SUPPORTED_SHORTENERS


class CustomHTTPAdapter(requests.adapters.HTTPAdapter):
    def __init__(
            self,
            fixed_timeout: int = 5,
            retries: int = 3,
            backoff_factor: float = 0.3,
            status_forcelist=(500, 502, 504),
            pool_maxsize: Optional[int] = None
    ):
        self._fixed_timeout = fixed_timeout
        retry_strategy = requests.packages.urllib3.util.retry.Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        kwargs = {'max_retries': retry_strategy}
        if pool_maxsize is not None:
            kwargs['pool_maxsize'] = pool_maxsize
        super().__init__(**kwargs)

    def send(self, *args, **kwargs):
        if kwargs['timeout'] is None:
            kwargs['timeout'] = self._fixed_timeout
        return super(CustomHTTPAdapter, self).send(*args, **kwargs)


class Nsq:
    def __init__(
            self,
            logger: logging.Logger,
            nsqd_tcp_address: str,
            topic: Optional = None,
            channel: Optional = None,
            handler: Optional[Callable[[Dict, nsq.Message, Callable[[str, Dict], None]], bool]] = None,
            failed_topic: Optional[str] = None,
            nsqd_read_port: int = 4150,
            nsqd_write_port: int = 4151,
            json_encoder: Optional = None,
    ):
        self._lock = threading.Lock()
        self._logger = logger
        self._failed_topic = F'{topic}.failed' if failed_topic is None else failed_topic
        self._nsqd_tcp_address = nsqd_tcp_address
        self._nsqd_write_port = nsqd_write_port
        self._json_encoder = json_encoder
        if topic and channel:
            self._reader = nsq.Reader(
                message_handler=self.handler,
                nsqd_tcp_addresses=[F'{self._nsqd_tcp_address}:{nsqd_read_port}'],
                topic=topic,
                channel=channel,
                user_agent=F'{nsq.conn.DEFAULT_USER_AGENT}/pid={os.getpid()}',
            )
            self._handler = handler

        self._session = requests.session()
        self._session.mount('https://', CustomHTTPAdapter())
        self._session.mount('http://', CustomHTTPAdapter())

    @staticmethod
    def start_reader():
        nsq.run()

    def handler(self, message: nsq.Message):
        with self._lock:
            decoded_body = message.body.decode('utf-8')
            try:
                decoded = json.loads(decoded_body)
            except json.decoder.JSONDecodeError:
                self.publish(self._failed_topic, message.body)
                return True  # do not requeue decoding errors
            if self._handler:
                return self._handler(decoded, message, self.publish_dict)
            else:
                print(json.dumps(decoded_body, indent=4, cls=self._json_encoder))
                return True

    def publish_dict(self, topic: str, message) -> None:
        return self.publish(topic, json.dumps(message, cls=self._json_encoder).encode('utf-8'))

    def publish(self, topic: str, message: bytes) -> None:
        response = self._session.post(
            F'http://{self._nsqd_tcp_address}:{self._nsqd_write_port}/pub?topic={topic}',
            data=message
        )
        response.raise_for_status()


class ScraperException(Exception):
    pass


@dataclasses.dataclass
class ExtractedPost:
    url: str
    post_datetime: datetime.datetime
    img_src: Optional[str]
    content: str


@dataclasses.dataclass
class ParsedPost:
    raw: ExtractedPost
    at_handles: List[str]
    external_urls: List[str]
    external_host_names: List[str]


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        elif dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        else:
            return super().default(obj)


@dataclasses.dataclass
class CrawlResponse:
    url: str
    content: bytes
    retrieval_date: datetime.datetime = datetime.datetime.now()


class BeautifulSoupUtils:
    def __init__(self, logger: logging.Logger):
        self._at_handle_regex = re.compile(r'@[a-z0-9]{1,100}', flags=re.IGNORECASE)
        self._url_regex = re.compile(r'(https?://[a-z0-9\-._~:/?#\[\]@!$&\'()*+,;%=]+)', flags=re.IGNORECASE)
        self._telegram_channel_link = re.compile(r'https://t.me/[^ ]{1,100}', flags=re.IGNORECASE)
        self._link_shortening_resolver = LinkShortener(logger)

    def extract_direct_text_content(self, container: bs4.element.Tag) -> str:
        content = ''
        for node in container.children:  # type: bs4.element.Tag
            if node.name == 'br':
                content += '\n'
            elif node.name in ['b', 'i']:
                content += self.extract_direct_text_content(node)
            elif node.name is None:
                content += node.text

        return content

    def extract_at_handles(self, content: str) -> List[str]:
        return self._at_handle_regex.findall(content)

    def extract_urls(self, content: str) -> Iterator[str]:
        for url in self._url_regex.findall(content):
            parsed = urllib.parse.urlparse(url)
            if parsed.hostname in ['t.me']:
                continue
            yield url
            if self._link_shortening_resolver.is_supported(url):
                yield self._link_shortening_resolver.resolve(url)

    @staticmethod
    def extract_hostnames(urls: List[str]) -> List[str]:
        return sorted(list(set([urllib.parse.urlparse(external_url).hostname for external_url in urls])))


class CrawlStrategy(ABC):
    def __init__(self, logger: logging.Logger, session: requests.Session, beautiful_soup_utils: BeautifulSoupUtils):
        self._logger = logger
        self._session = session
        self._beautiful_soup_utils = beautiful_soup_utils

    def crawl(self, channel_name: str) -> Iterator[CrawlResponse]:
        raise NotImplementedError

    def extract_posts(self, crawl_response: CrawlResponse) -> Iterator[ExtractedPost]:
        raise NotImplementedError


class StoreStrategy(CrawlStrategy):
    BASE_URL = 'https://telegram-store.com'

    def __init__(self, logger: logging.Logger, session: requests.Session, beautiful_soup_utils: BeautifulSoupUtils):
        self._page_parameter_regex = re.compile(r'\?page=(\d{1,3})')
        super().__init__(logger, session, beautiful_soup_utils)

    def crawl(self, channel_name: str) -> Iterator[CrawlResponse]:
        url = f'{self.BASE_URL}/catalog/channels/{channel_name}'
        self._logger.debug(f'Retrieving "{url}"...')
        response = self._session.get(url)
        response.raise_for_status()

        soup = bs4.BeautifulSoup(response.content, 'html.parser')
        pagination = soup.find('ul', attrs={'class': 'page-numbers'})
        max_page = max(
            int(self._page_parameter_regex.search(a.attrs['href']).group(1), 10) for a in pagination.find_all('a')
        )
        actual_url = response.url
        yield CrawlResponse(actual_url, response.content)
        assert '?' not in actual_url

        for i in range(2, max_page + 1):
            page_url = f'{actual_url}?page={i}'
            self._logger.debug(f'Retrieving "{page_url}"...')
            response = self._session.get(page_url)
            response.raise_for_status()
            yield CrawlResponse(response.url, response.content)

    def extract_posts(self, crawl_response: CrawlResponse) -> Iterator[ExtractedPost]:
        soup = bs4.BeautifulSoup(crawl_response.content, 'html.parser')
        for span in soup.find_all('span', attrs={'class': 'date_in_messages'}):
            container = span.parent
            img = container.find('img')
            content = self._beautiful_soup_utils.extract_direct_text_content(container)
            yield ExtractedPost(
                url=crawl_response.url,
                post_datetime=datetime.datetime.strptime(span.text, '%Y-%m-%d %H:%M:%S'),
                img_src=img.attrs['data-src'] if img else None,
                content=content.strip(),
            )


class PreviewStrategy(CrawlStrategy):
    BASE_URL = 'https://t.me'

    def crawl(self, channel_name: str) -> Iterator[CrawlResponse]:
        url = f'{self.BASE_URL}/s/{channel_name}'
        while url:
            self._logger.debug(f'Retrieving "{url}"...')
            response = self._session.get(url)
            response.raise_for_status()
            yield CrawlResponse(response.url, response.content)
            soup = bs4.BeautifulSoup(response.content, 'html.parser')
            prev_link = soup.find('link', {'rel': 'prev'})
            if prev_link is None or prev_link.attrs is None:
                break
            url = prev_link.attrs['href']
            if url.startswith('/'):
                url = f'{self.BASE_URL}{url}'

    def extract_posts(self, crawl_response: CrawlResponse) -> Iterator[ExtractedPost]:
        soup = bs4.BeautifulSoup(crawl_response.content, 'html.parser')
        for div in soup.find_all('div'):
            if div.attrs is None or 'data-post' not in div.attrs.keys():
                continue

            url = f'{self.BASE_URL}/s/{div.attrs["data-post"]}'

            img_src = None
            for a in div.find_all('a', class_='tgme_widget_message_photo_wrap'):
                for entry in a.attrs['style'].split(';'):
                    name, value = entry.split(':', maxsplit=1)
                    if name == 'background-image' and value.startswith("url('") and value.endswith("')"):
                        img_src = value[5:-2]

            post_datetime = None
            for time_tag in div.find_all('time'):
                if time_tag.attrs is None or 'datetime' not in time_tag.attrs.keys():
                    continue
                post_datetime = datetime.datetime.fromisoformat(time_tag.attrs['datetime'])
            if post_datetime is None:
                continue

            text_divs = [
                text_div for text_div in div.select('div[class*="tgme_widget_message_text"]')
                if 'js-message_reply_text' not in text_div.attrs['class']
            ]
            content = ''
            for text_div in text_divs:
                content += self._beautiful_soup_utils.extract_direct_text_content(text_div) + '\n\n'

            yield ExtractedPost(
                url=url,
                img_src=img_src,
                post_datetime=post_datetime,
                content=content.strip()
                if len(text_divs) == 1 else '',
            )


class TelegramScraper:
    def __init__(self, logger: logging.Logger, user_agent: str, strategy_class: Type[CrawlStrategy]):
        self._logger = logger
        self._session = requests.session()
        self._session.mount('https://', CustomHTTPAdapter())
        self._session.mount('http://', CustomHTTPAdapter())
        self._session.headers = {'User-Agent': user_agent}
        self._beautiful_soup_utils = BeautifulSoupUtils(logger)

        self._strategy = strategy_class(logger, self._session, self._beautiful_soup_utils)

    def crawl(self, channel_name: str) -> Iterator[CrawlResponse]:
        yield from self._strategy.crawl(channel_name)

    def _parse_post(self, extracted_post: ExtractedPost) -> ParsedPost:
        external_urls = list(self._beautiful_soup_utils.extract_urls(extracted_post.content))
        return ParsedPost(
            raw=extracted_post,
            at_handles=self._beautiful_soup_utils.extract_at_handles(extracted_post.content),
            external_urls=external_urls,
            external_host_names=self._beautiful_soup_utils.extract_hostnames(external_urls),
        )

    def posts(self, crawl_response: CrawlResponse) -> Iterator[ParsedPost]:
        for extracted_post in self._strategy.extract_posts(crawl_response):
            yield self._parse_post(extracted_post)


class ConsoleHandler(logging.Handler):
    def emit(self, record):
        print('[%s] %s' % (record.levelname, record.msg))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage', choices=['file', 'nsq'], default='file')
    parser.add_argument('--directory', default=os.path.join(os.path.dirname(__file__), 'data'))
    parser.add_argument('--debug', action='store_true')
    parser.add_argument(
        '--user-agent',
        default=F'PythonTelegramScraper/{__version__} (python-requests {requests.__version__}) '
                F'{platform.system()} ({platform.release()})'
    )
    parser.add_argument('--strategy', choices=['store', 'preview'], default='store')
    parser.add_argument('--nsqd-tcp-address', default=os.environ.get('NSQD_TCP_ADDRESS', '127.0.0.1'))
    subparsers = parser.add_subparsers(dest='command')
    crawl_channel_parser = subparsers.add_parser('crawl_channel')
    crawl_channel_parser.add_argument('channel_name')
    extract_parser = subparsers.add_parser('extract')
    extract_parser.add_argument('--channel_name')

    args = parser.parse_args()

    if args.storage == 'file':
        if not os.path.exists(args.directory):
            os.mkdir(args.directory)

    logger = logging.getLogger('TelegramScraper')
    logger.handlers.append(ConsoleHandler())
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    logger.debug(F'Using User-Agent string: {args.user_agent}')
    if args.strategy == 'preview':
        strategy = PreviewStrategy
    elif args.strategy == 'store':
        strategy = StoreStrategy
    else:
        raise Exception(f'Invalid strategy: {args.crawl_strategy}')
    scraper = TelegramScraper(logger, args.user_agent, strategy)
    handler = Nsq(logger, args.nsqd_tcp_address, json_encoder=CustomJSONEncoder)
    try:
        if args.command == 'crawl_channel':
            for response in scraper.crawl(args.channel_name):
                if args.storage == 'file':
                    file_name = binascii.hexlify(response.url.encode('utf-8')).decode('utf-8') + '.json'
                    with open(os.path.join(args.directory, file_name), 'w') as fp:
                        json.dump(response, fp, indent=4, cls=CustomJSONEncoder)
                else:
                    handler.publish_dict(args.nsq_topic_crawled_content, response)
                for post in scraper.posts(response):
                    logger.debug(post)
        elif args.command == 'extract':
            at_handles = Counter()
            host_names = Counter()
            for file_name in os.listdir(args.directory):
                with open(os.path.join(args.directory, file_name), 'r') as fp:
                    file_content = json.load(fp)
                response = CrawlResponse(
                    url=file_content['url'],
                    content=file_content['content'],
                    retrieval_date=datetime.datetime.fromisoformat(file_content['retrieval_date']),
                )
                for post in scraper.posts(response):
                    for at_handle in post.at_handles:
                        at_handles[at_handle] += 1
                    for host_name in post.external_host_names:
                        host_names[host_name] += 1

            print('# @handles')
            for at_handle, count in at_handles.most_common():
                print(at_handle, count)
            print('')
            print('# domains')
            for host_name, count in host_names.most_common():
                print(host_name, count)
        else:
            raise Exception(f'Unknown command: {args.command}')
    except ScraperException as e:
        logger.exception(e)
    except requests.exceptions.ConnectionError as e:
        logger.exception(e)


if __name__ == '__main__':
    main()
