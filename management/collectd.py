#!/usr/bin/python

import sys, socket, re
from datetime import datetime


class CollectdError(Exception):
    pass


class Identifier(object):
    _name_re = re.compile('(?P<hostname>[^/]+)/'
                          '(?P<plugin>[^-/]+)(?:-(?P<plugin_instance>[^/]+))?/'
                          '(?P<type>[^-/]+)(?:-(?P<type_instance>[^/]+))?$')
    def __init__(self, hostname, plugin, plugin_instance, type, type_instance):
        self.hostname = hostname
        self.plugin = plugin
        self.plugin_instance = plugin_instance
        self.type = type
        self.type_instance = type_instance

    def __repr__(self):
        return '<{0}({1})>'.format(self.__class__.__name__, self.format_name())

    def __str__(self):
	return self.format_name()

    @classmethod
    def fromstring(cls, s):
        m = cls._name_re.match(s)
        return cls(**m.groupdict()) if m else None

    def todict(self):
        return self.__dict__

    def totuple(self):
        return self.hostname, self.plugin, self.plugin_instance, self.type, self.type_instance

    def format_instance(self, a, b):
        return '{0}-{1}'.format(a, b) if b is not None else a

    def format_plugin(self):
        return self.format_instance(self.plugin, self.plugin_instance)

    def format_type(self):
        return self.format_instance(self.type, self.type_instance)

    def format_name(self):
        return '/'.join((self.hostname, self.format_plugin(), self.format_type()))

    def fetch(self):
        lines = self.collectd.getval(self.format_name())
        return dict((line.split('=') for line in lines))


class UnixSock(object):
    def __init__(self, socket_name='/var/run/collectd-unixsock'):
        self._sock = socket.socket(socket.AF_UNIX)
        self._sock.connect(socket_name)

    def _send(self, data):
        self._sock.sendall(data)

    def _recvline(self):
        data = ''
        while not data or data[-1] != '\n':
            data = data + self._sock.recv(4096)

        return data

    def _recvlines(self):
        lines = ['']
        while len(lines) == 1 or lines[-1] != '' or lines[0] and len(lines) <= int(lines[0].split()[0]) + 1:
            data = self._sock.recv(4096)
            if not data:
                continue
            new_lines = data.split('\n')
            lines[-1] += new_lines[0]
            lines.extend(new_lines[1:])

        return lines[1:-1]

    def _format_options(self, options):
        """
        Format a dictionary of options as a list of key=value pairs
        starting with a space.

        """

        return ''.join((' ' + '{0}={1}'.format(k, v) for k, v in options.iteritems()))

    @staticmethod
    def _parse_values(lines):
        kviter = (line.split('=') for line in lines)
        return dict(((k, float(v)) for k, v in kviter))

    @staticmethod
    def _parse_line(line):
        timestamp, name = line.split(' ', 1)
        return datetime.fromtimestamp(float(timestamp)), Identifier.fromstring(name)

    def listval(self):
        self._send('LISTVAL\n')
        lines = self._recvlines()
        return (self._parse_line(line) for line in lines)

    def getval(self, name):
        self._send('GETVAL {0}\n'.format(name))
        return self._parse_values(self._recvlines())

    def putval(self, name, time, values, **kwargs):
        cmd = 'PUTVAL {0}{1} {2}:{3}\n'.format(name, self._format_options(kwargs), int(time), ':'.join((str(v) for v in values)))
        self._send(cmd)
        result = self._recvline()
        status = result.split()[0]
        if status != '0':
            raise CollectdError, result

    def putnotif(self, time, name, severity, message):
        if not isinstance(name, Identifier):
            name = Identifier.fromstring(name)

        self._send('PUTVAL time={0} severity={1}{2} message={3}\n'.format(time, severity, self._format_options(name.todict())))


def main():
    c = UnixSock()
    for timestamp, name in c.listval():
        print name, timestamp, c.getval(name)


if __name__ == '__main__':
    main()

