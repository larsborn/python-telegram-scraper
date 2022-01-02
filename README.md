# Python Telegram Scraper (pts)

This is a scraper doesn't use the Telegram API but scrapes the web frontend. Hence, it doesn't require any API
credentials. It currently supports the `telegram-store.com` web frontend as well as `t.me`. It seems that the first of
the two only supports scrolling back to a certain point, while `t.me` allows for retrieval of the entire content of a
channel.

## Installation

Use a virtual environment:

```bash
pip install -r requirements.txt
```

## Usage Examples

The following will use `t.me` to scroll through the entire supplied channel and store all results in a directory called
`output`.

```bash
./pts.py --storage file --directory output --strategy preview crawl_channel YOUR_CHANNEL_NAME 
```

Instead of `file`, you can pass `nsq` to publish to an NSQ instead of the local file system. Pass an appropriate value
for `--nsqd-tcp-address` in that case.

```bash
./pts.py extract
```

The above line will print some stats of the parsed data (if stored on disk) to quickly gauge related channels and
referenced websites. It will also print a histogram across times of day to be able to easily guess the activity time
zone of a channel.
