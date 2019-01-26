#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    graceful stop
"""
import os
import signal
import multiprocessing
import logging


class GracefulExitException(Exception):
    @staticmethod
    def sigterm_handler(signum, frame):
        raise GracefulExitException()
    pass


class GracefulExitEvent(object):
    def __init__(self):
        self.workers = []
        self.exit_event = multiprocessing.Event()

        # Use signal handler to throw exception which can be caught
        # by worker process to allow graceful exit.
        signal.signal(signal.SIGTERM, GracefulExitException.sigterm_handler)
        signal.signal(signal.SIGINT, GracefulExitException.sigterm_handler)
        pass

    def reg_worker(self, wp):
        self.workers.append(wp)
        pass

    def is_stop(self):
        return self.exit_event.is_set()

    def notify_stop(self):
        self.exit_event.set()

    def wait_all(self):
        while True:
            try:
                for wp in self.workers:
                    wp.join()

                logging.info("main process({}) exit.".format(os.getpid()))
                break
            except GracefulExitException:
                self.notify_stop()
                logging.info("main process({}) got GracefulExitException.".format(os.getpid()))
            except Exception, ex:
                self.notify_stop()
                logging.info("main process({}) got unexpected Exception: {}".format(os.getpid(), ex))
                break
