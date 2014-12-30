# vim: tabstop=8
# vim: expandtab
# vim: shiftwidth=4
# vim: softtabstop=4

# stdlib
import subprocess
import os
import signal
import csv
import datetime
import time
from hashlib import md5

# project
from checks import AgentCheck

class TCPRoundtripLatencyCheckTimeout(Exception):
    pass

class TCPRoundtripLatencyCheck(AgentCheck):
    NETPERF_BIN = "/usr/bin/netperf"
    NETPERF_PROTOCOL = "TCP"
    NETPERF_TEST_DURATION_SECONDS = 1
    NETPERF_OUTPUT_COLS = "PROTOCOL,RT_LATENCY,P90_LATENCY,P99_LATENCY"

    def __init__(self, name, init_config, agentConfig):
        AgentCheck.__init__(self, name, init_config, agentConfig)

    def check(self, instance):
        host, port, tcp_request_size_bytes, tcp_response_size_bytes, timeout, tags, options = self._get_config(instance)

        if (not host or port == 0 or tcp_request_size_bytes == 0 or
                tcp_response_size_bytes == 0):
            raise Exception("Netperf server host is needed.")

        # Use a hash of the host as an aggregation key
        aggregation_key = md5(host).hexdigest()

        # Metrics collection
        self._collect_metrics(host, port, tcp_request_size_bytes, tcp_response_size_bytes, timeout, tags, aggregation_key)

    def _get_config(self, instance):
        host = instance.get('host', '')
        port = instance.get('port', 0)
        tcp_request_size_bytes = instance.get('tcp_request_size_bytes', 0)
        tcp_response_size_bytes = instance.get('tcp_response_size_bytes', 0)
        timeout = float(instance.get('timeout', 5))

        tags = instance.get('tags', None)
        options = instance.get('options', {})

        return host, port, tcp_request_size_bytes, tcp_response_size_bytes, timeout, tags, options

    def _collect_metrics(self, host, port, tcp_request_size_bytes, tcp_response_size_bytes, timeout, tags, aggregation_key):
        """ Netperf command used is as follows:
        /usr/bin/netperf -l 1 -H 192.168.10.10 -p 12865 -t omni -j -v 0 -P 0 -- -d rr -o PROTOCOL,RT_LATENCY,P90_LATENCY,P99_LATENCY -r 512,256 -T TCP -b 6

        The output is in CSV format without headers:
        TCP,667.240,785,970

        The columns are ordered based on what is passed to the -o flag.
        For example, in the sample command above we pass "-o PROTOCOL,RT_LATENCY,P90_LATENCY,P99_LATENCY".
        This means that the first column in the output is PROTOCOL and the last column is P99_LATENCY"""

        netperf_cmd = "%s -l %d -H %s -p %s -t omni -j -v 0 -P 0 -- -d rr -o %s -r %d,%d -T %s -b 6" % (
                                                        self.NETPERF_BIN,
                                                        self.NETPERF_TEST_DURATION_SECONDS, host, port,
                                                        self.NETPERF_OUTPUT_COLS, tcp_request_size_bytes,
                                                        tcp_response_size_bytes, self.NETPERF_PROTOCOL)

        try:
            res = self._timeout_command(netperf_cmd, timeout)
        except RuntimeError as e:
            self.error_event(host, str(e), aggregation_key)
            return
        except TCPRoundtripLatencyCheckTimeout as e:
            self.timeout_event(host, timeout, aggregation_key)
            return

        for protocol, rt_latency, p90_latency, p99_latency in csv.reader([res]):
            pass

        rt_latency = float(rt_latency)
        self.histogram("mysql.net.tcp_rt_latency", rt_latency, tags=tags)

    def _timeout_command(self, command, timeout):
        """call shell-command and either return its output or kill it
        if it doesn't normally exit within timeout seconds and return False"""

        cmd = command.split(" ")
        start = datetime.datetime.now()

        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Command '%s' failed. "
                    "Return code: %s.\nOutput:\n %s." % (command, e.returncode,
                                                        "\n ".join(e.output.splitlines())))

        while process.poll() is None:
            time.sleep(0.1)
            now = datetime.datetime.now()

            if (now - start).seconds > timeout:
                os.kill(process.pid, signal.SIGKILL)
                os.waitpid(-1, os.WNOHANG)
                raise TCPRoundtripLatencyCheckTimeout("The command '%s' timed out after %d seconds" % (command, timeout))

        return process.stdout.read()

    def timeout_event(self, host, timeout, aggregation_key):
        self.event({
            'timestamp': int(time.time()),
            'event_type': 'mysql_tcp_rt_latency_check',
            'msg_title': 'MySQL host %s to remote host RT latency timeout' % host,
            'msg_text': 'TCP connection attempt to %s timed out after %s seconds.' % (host, timeout),
            'aggregation_key': aggregation_key
        })

    def error_event(self, host, error_msg, aggregation_key):
        self.event({
            'timestamp': int(time.time()),
            'event_type': 'mysql_tcp_rt_latency_check',
            'msg_title': 'MySQL host %s to remote host RT latency command returned invalid code' % host,
            'msg_text': '%s' % error_msg,
            'aggregation_key': aggregation_key
        })


if __name__ == '__main__':
    check, instances = TCPRoundtripLatencyCheck.from_yaml('/etc/dd-agent/conf.d/tcp_roundtrip_latency_check.yaml')
    for instance in instances:
        print "\nRunning the check against host: %s" % instance.get('host')

        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())

        print 'Metrics: %s' % (check.get_metrics())

