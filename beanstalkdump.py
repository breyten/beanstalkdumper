#!/usr/bin/env python

import sys
import os
import re
from pprint import pprint
import getopt
from time import sleep

import logbook
import beanstalkc

log = logbook.Logger(os.path.basename(__file__)
                     if __name__ == "__main__"
                     else __name__)


class BeanstalkWorker(object):
    def __init__(self, host, port, delay, tubes):
        self.host = host
        self.port = port
        self.beanstalk = None
        self.delay = delay
        self.tubes = tubes

    def _connect(self):
        log.info(
            "Beanstalk connection: {host}:{port}",
            host=self.host,
            port=self.port
        )

        if self.beanstalk is None:
            self.beanstalk = beanstalkc.Connection(
                host=self.host,
                port=self.port
            )

    def _process(self, job):
        stats = job.stats()
        log.notice(
            "[{tube}] New job : {id}",
            id=job.jid, tube=stats['tube']
        )

    def run(self):
        self._connect()

        log.info(
            "Delay : {delay} seconds, Watching: {tubes}",
            delay=self.delay, tubes=self.tubes
        )

        for tube in self.tubes:
            self.beanstalk.watch(tube)

        while True:
            sleep(self.delay)
            job = self.beanstalk.reserve(timeout=0)
            if job:
                self._process(job)
                job.delete()


def main(argv=None):
    loglevel = logbook.INFO
    loglevel_name = "INFO"
    output = '-'
    host = "localhost"
    port = 11300
    delay = 0.2
    tubes = ['default']

    if argv is None:
        argv = sys.argv

    try:
        try:
            opts, args = getopt.getopt(
                argv[1:],
                "hl:o:H:p:d:t:",
                [
                    "help", "loglevel=", "output=", "host=", "port=", "delay=",
                    "tubes="
                ]
            )
        except getopt.error, msg:
            raise Usage(msg)

        # option processing
        for option, value in opts:
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-l", "--loglevel"):
                loglevel_name = value.upper()
                if not hasattr(logbook, loglevel_name):
                    raise Usage("Invalid {0} value: {1}".format(option, value))
                loglevel = getattr(logbook, loglevel_name)
                if not isinstance(loglevel, int):
                    raise Usage("Invalid {0} value: {1}".format(option, value))
            if option in ("-o", "--output"):
                output = value
            if option in ("-H", "--host"):
                host = value
            if option in ("-p", "--port"):
                port = int(value)
            if option in ("-d", "--delay"):
                delay = float(value)
            if option in ("-t", "--tubes"):
                tubes = re.split(r'\s*,\s*', value)

    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2

    if output == 'syslog':
        log_handler = logbook.SyslogHandler(
            application_name='beanstalk-dumper',
            bubble=False,
            level=loglevel)
    elif output == '-' or not output:
        log_handler = logbook.StderrHandler(
            level=loglevel,
            bubble=False)
    else:
        log_handler = logbook.FileHandler(
            filename=output,
            encoding='utf-8',
            level=loglevel,
            bubble=False)

    with logbook.NullHandler():
        with log_handler.applicationbound():
            log.info("Starting Beanstalk dumper...")
            log.notice("Log level {0}".format(loglevel_name))
            app = BeanstalkWorker(host, port, delay, tubes)
            return app.run()


if __name__ == '__main__':
    sys.exit(main())
