#!/usr/bin/python2.4
# Some versions of feedparser have a bug that makes it throw
# UnicodeDecodeError exceptions when run under Python 2.5, so I'm using 2.4
# for now.

import calendar
import feedparser
import os
import re
import sys
import time
from email.Generator import Generator
from optparse import OptionParser
from pysqlite2 import dbapi2 as sqlite

'''
CREATE TABLE Feeds (
  FeedId INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  Url VARCHAR(256) UNIQUE NOT NULL,
  ShortName VARCHAR(256),
  Description VARCHAR(256),
  LastFetched INTEGER
);

CREATE TABLE FeedItems (
  FeedId INTEGER NOT NULL,
  Guid VARCHAR(256) NOT NULL,
  WhenSeen INTEGER,
  PRIMARY KEY (FeedId, Guid)
);

CREATE TABLE Subscriptions (
  FeedId INTEGER,
  Email VARCHAR(256) NOT NULL,
  Active BOOLEAN,
  PRIMARY KEY (FeedId, Email)
);

CREATE TABLE Filters (
  FeedId INTEGER,
  RegExp VARCHAR(256) NOT NULL,
  PRIMARY KEY (FeedId, RegExp)
);
'''

class CraigSniper:
    def __init__(self, db_filename, sendmail='/usr/lib/sendmail -t',
                 verbose=False):
        self._db = sqlite.connect(db_filename)
        self._sendmail = sendmail
        self._verbose = verbose

    def __seen_item(self, feed_id, item):
        query = 'SELECT COUNT(*) FROM FeedItems WHERE FeedId = ? AND Guid = ?'
        cur = self._db.cursor()
        cur.execute(query, (feed_id, item.guid))
        return cur.fetchone()[0] > 0

    def __record_item(self, feed_id, item):
        query = 'INSERT INTO FeedItems (FeedId, Guid, WhenSeen) VALUES(?, ?, ?)'
        self._db.cursor().execute(query, (feed_id, item.guid, int(time.time())))

    def __update_last_fetched(self, feed_id):
        query = 'UPDATE Feeds SET LastFetched = ? WHERE FeedId = ?'
        self._db.cursor().execute(query, (int(time.time()), feed_id))

    def __get_subscriptions(self):
        subscriptions = {}
        query = 'SELECT FeedId, Email FROM Subscriptions WHERE Active = 1'
        cur = self._db.cursor()
        for feed_id, email in cur.execute(query):
            subscriptions.setdefault(feed_id, []).append(email)
        return subscriptions

    def __get_filters(self):
        filters = {}
        query = 'SELECT FeedId, RegExp FROM Filters'
        cur = self._db.cursor()
        for feed_id, regexp in cur.execute(query):
            filters.setdefault(feed_id, []).append(regexp)
        return filters

    def __generate_message(self, item, short_name):
        subject_header = ''
        if short_name: subject_header = '[%s] ' % short_name

        message = '''\
From: craigsnipe@erat.org
To: !to!
Subject: %s%s
Content-Type: text/html

%s

<hr/>

<p>This item was posted at %s.<br/>
View the original at <a href="%s">%s</a>.''' % \
            (subject_header,
             item.title.replace('&amp;', '&').encode('ascii', 'replace'),
             item.description.encode('ascii', 'replace'),
             time.ctime(calendar.timegm(item.date_parsed)),
             item.link, item.link)
        return message

    def __email_message(self, message, address, dry_run=False):
        message = message.replace('!to!', address)
        if dry_run:
            print message + "\n\n" + ('-' * 80) + "\n"
        else:
            p = os.popen(self._sendmail, 'w')
            p.write(message)
            code = p.close()
            if code:
                print '%s exited with ' + code

    def __vlog(self, msg):
        if self._verbose:
            print msg

    def process_feeds(self, dry_run=False):
        subscriptions = self.__get_subscriptions()
        filters = self.__get_filters()
        query = 'SELECT FeedId, ShortName, Url FROM Feeds'
        cur = self._db.cursor()
        for feed_id, short_name, url in cur.execute(query):
            self.__vlog('Processing feed %s' % url)
            if not subscriptions.has_key(feed_id):
                self.__vlog('Skipping %s with no subscribers' % url)
                continue
            subscribers = subscriptions[feed_id]
            feed = feedparser.parse(url)
            for item in feed.entries:
                self.__vlog('Got item %s' % item.guid)
                if not dry_run and self.__seen_item(feed_id, item):
                    self.__vlog('Skipping %s because we\'ve already seen it' %
                                item.guid)
                    continue

                matched_by_filter = False
                for regexp in filters.get(feed_id, []):
                    if re.search(regexp, item.description, re.IGNORECASE):
                        self.__vlog('Skipping %s because it matches filter "%s"' %
                                    (item.guid, regexp))
                        matched_by_filter = True
                        break
                if matched_by_filter:
                    continue

                try:
                    message = self.__generate_message(item, short_name)
                except AttributeError:
                    self.__vlog('Skipping %s because because of exception' % item.guid)
                    continue
                for address in subscribers:
                    self.__vlog('Sending mail to %s' % address)
                    self.__email_message(message, address, dry_run)
                if not dry_run: self.__record_item(feed_id, item)
            if not dry_run: self.__update_last_fetched(feed_id)
        if not dry_run: self._db.commit()


def main():
    parser = OptionParser()
    parser.add_option('-d', '--db', help='sqlite3 database file',
                      default='data/craigsnipe.db', metavar='PATH', dest='db')
    parser.add_option('-f', '--from', help='Source email address',
                      default=None, action='store_true', dest='from_address')
    parser.add_option('-n', '--dry-run', help='Dry-run mode: '
                      'don\'t send email or update the DB, and process items '
                      'that have already been seen', default=False,
                      action='store_true', dest='dry_run')
    parser.add_option('-s', '--sendmail', help='Path and args for sendmail',
                      default='/usr/lib/sendmail -t', dest='sendmail')
    parser.add_option('-v', '--verbose', help='Verbose', dest='verbose',
                      default=False, action='store_true')
    (options, args) = parser.parse_args()

    if not os.path.exists(options.db):
        sys.stderr.write('Database %s not found' % options.db)
        sys.exit(1)
    craig = CraigSniper(options.db,
                        sendmail=options.sendmail,
                        verbose=options.verbose)
    craig.process_feeds(options.dry_run)

if __name__ == "__main__":
    main()
