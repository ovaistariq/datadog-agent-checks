# (c) 2014, Ovais Tariq <me@ovaistariq.net>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# vim: tabstop=8
# vim: expandtab
# vim: shiftwidth=4
# vim: softtabstop=4

# stdlib
import subprocess
import os
import sys
import re
import traceback

# project
from checks import AgentCheck
from util import Platform

# 3rd party
import pymysql

GAUGE = "gauge"
RATE = "rate"

METRICS_MAP = {
    'Ps_digest_95th_percentile_by_avg_us': ('mysql.sys.query_exec_time_95th_per_us', GAUGE)
}

class MySqlSys(AgentCheck):
    def __init__(self, name, init_config, agentConfig):
        AgentCheck.__init__(self, name, init_config, agentConfig)
        self.schema_name = 'sys'

    def get_library_versions(self):
        return {"pymysql": pymysql.__version__}

    def check(self, instance):
        host, port, user, password, mysql_sock, defaults_file, tags, options = self._get_config(instance)

        if (not host or not user) and not defaults_file:
            raise Exception("Mysql host and user are needed.")

        db = self._connect(host, port, mysql_sock, user, password, defaults_file)

        # check that we are running the correct MySQL version
        if not self._version_greater_565(db, host):
            raise Exception("MySQL version >= 5.6.5 is required.")

        # check that mysql_sys is installed
        if not self._is_mysql_sys_schema_installed(db):
            raise Exception("The mysql_sys utility is not installed. Please visit https://github.com/MarkLeith/mysql-sys for installation instructions")

        # Metric collection
        self._collect_metrics(host, db, tags, options)

    def _get_config(self, instance):
        host = instance.get('server', '')
        user = instance.get('user', '')
        port = int(instance.get('port', 0))
        password = instance.get('pass', '')
        mysql_sock = instance.get('sock', '')
        defaults_file = instance.get('defaults_file', '')
        tags = instance.get('tags', None)
        options = instance.get('options', {})

        return host, port, user, password, mysql_sock, defaults_file, tags, options

    def _connect(self, host, port, mysql_sock, user, password, defaults_file):
        if defaults_file != '':
            db = pymysql.connect(read_default_file=defaults_file,
                                    db=self.schema_name)
        elif  mysql_sock != '':
            db = pymysql.connect(unix_socket=mysql_sock,
                                    user=user,
                                    passwd=password,
                                    db=self.schema_name)
        elif port:
            db = pymysql.connect(host=host,
                                    port=port,
                                    user=user,
                                    passwd=password,
                                    db=self.schema_name)
        else:
            db = pymysql.connect(host=host,
                                    user=user,
                                    passwd=password,
                                    db=self.schema_name)
        self.log.debug("Connected to MySQL")

        return db

    def _collect_metrics(self, host, db, tags, options):
        mysql_sys_metrics = dict()

        # Compute 95ht percentile query execution time in microseconds
        mysql_sys_metrics['Ps_digest_95th_percentile_by_avg_us'] = self._get_query_exec_time_95th_per_us(db)
        
        # Send the metrics to Datadog based on the type of the metric
        self._rate_or_gauge_statuses(METRICS_MAP, mysql_sys_metrics, tags)

    def _rate_or_gauge_statuses(self, statuses, dbResults, tags):
        for status, metric in statuses.iteritems():
            metric_name, metric_type = metric
            value = self._collect_scalar(status, dbResults)
            if value is not None:
                if metric_type == RATE:
                    self.rate(metric_name, value, tags=tags)
                elif metric_type == GAUGE:
                    self.gauge(metric_name, value, tags=tags)

    def _get_query_exec_time_95th_per_us(self, db):
        # Fetches the 95th percentile query execution time and returns the value
        # in microseconds

        cursor = db.cursor()
        cursor.execute("select * from x$ps_digest_95th_percentile_by_avg_us")

        if cursor.rowcount != 1:
            raise Exception("Failed to fetch record from the table x$ps_digest_95th_percentile_by_avg_us")

        row = cursor.fetchone()
        query_exec_time_95th_per = row[0]

        return query_exec_time_95th_per


    def _version_greater_565(self, db, host):
        # some of the performance_schema tables such as events_statements_%
        # tables were only introduced in MySQL 5.6.5. For reference see this
        # this link from the manual: 
        # http://dev.mysql.com/doc/refman/5.6/en/performance-schema-statement-digests.html
        # some patch version numbers contain letters (e.g. 5.0.51a)
        # so let's be careful when we compute the version number
        greater_565 = False
        try:
            mysql_version = self._get_version(db, host)
            self.log.debug("MySQL version %s" % mysql_version)

            major = int(mysql_version[0])
            minor = int(mysql_version[1])
            patchlevel = int(re.match(r"([0-9]+)", mysql_version[2]).group(1))

            if (major, minor, patchlevel) > (5, 6, 5):
                greater_565 = True

        except Exception, exception:
            self.warning("Cannot compute mysql version, assuming older than 5.6.5: %s" % str(exception))

        return greater_565

    def _get_version(self, db, host):
        # Get MySQL version
        cursor = db.cursor()
        cursor.execute('SELECT VERSION()')
        result = cursor.fetchone()
        cursor.close()
        del cursor
        # Version might include a description e.g. 4.1.26-log.
        # See http://dev.mysql.com/doc/refman/4.1/en/information-functions.html#function_version
        version = result[0].split('-')
        version = version[0].split('.')
        return version

    def _is_mysql_sys_schema_installed(self, db):
        cursor = db.cursor()
        return_val = False
        
        cursor.execute("select sys_version from version")
        if cursor.rowcount > 0:
            return_val = True

        cursor.close()
        del cursor

        return return_val

    def _collect_scalar(self, key, dict):
        return self._collect_type(key, dict, float)

    def _collect_string(self, key, dict):
        return self._collect_type(key, dict, unicode)

    def _collect_type(self, key, dict, the_type):
        self.log.debug("Collecting data with %s" % key)
        if key not in dict:
            self.log.debug("%s returned None" % key)
            return None
        self.log.debug("Collecting done, value %s" % dict[key])
        return the_type(dict[key])

