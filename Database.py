import paramiko
import psycopg2
from knob_config.parse_knob_config import get_knobs


class Database:
    def __init__(self, config, path):
        self.host = config['database_config']['host']
        self.port = int(config['database_config']['port'])
        self.database = config['database_config']['database']
        self.user = config['database_config']['user']
        self.password = config['database_config']['password']
        self.data_path = config['database_config']['data_path']
        self.knobs = get_knobs(path)
        self.ssh = None
        self.get_ssh(config)

    def get_conn(self):
        conn = psycopg2.connect(database=self.database,
                                user=self.user,
                                password=self.password,
                                host=self.host,
                                port=int(self.port))
        return conn

    def get_ssh(self, config):
        if self.ssh is not None:
            return self.ssh
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=config['ssh_config']['host'],
                         port=int(config['ssh_config']['port']),
                         username=config['ssh_config']['user'],
                         password=config['ssh_config']['password']
                         )
        print("connect to the database host...")

    def fetch_knob(self):
        conn = self.get_conn()
        knobs = {}
        cursor = conn.cursor()
        for knob in self.knobs:
            sql = "SELECT name, setting FROM pg_settings WHERE name='{}'".format(knob)
            cursor.execute(sql)
            result = cursor.fetchall()
            for s in result:
                knobs[knob] = float(s[1])
        cursor.close()
        conn.close()
        return knobs

    def fetch_inner_metric(self):
        state_list = []
        conn = self.get_conn()
        cursor = conn.cursor()

        # ['cpu_useage','memory_useage','kB_rd/s','kB_wr/s','cache_hit_rate','concurrent_users','lock_wait_count','error_rate','logical_reads_per_second','physical_reads_per_second','active_session','transactions_per_second','rows_scanned_per_second','rows_updated_per_second','rows_deleted_per_second']
        # cpu和内存占用率s
        stdin, stdout, stderr = self.ssh.exec_command("top -b -n 1")
        lines = stdout.readlines()
        gaussdb_line = None
        for line in lines:
            if 'gaussdb' in line:
                gaussdb_line = line
                break
        if gaussdb_line:
            columes = gaussdb_line.split()
            cpu_usage = columes[8]
            state_list.append(cpu_usage)
            mem_usage = columes[9]
            state_list.append(mem_usage)
        else:
            print("gaussdb process not found in top output.")

        # 每秒读取和写入的kB数，kB_rd/s,kB_wr/s
        stdin, stdout, stderr = self.ssh.exec_command("pidstat -d")
        lines = stdout.readlines()[1:]
        gaussdb_line = None
        for line in lines:
            if 'gaussdb' in line:
                gaussdb_line = line
                break
        if gaussdb_line:
            columes = gaussdb_line.split()
            kB_rd = columes[3]
            state_list.append(kB_rd)
            kB_wr = columes[4]
            state_list.append(kB_wr)
        else:
            print("gaussdb process not found in pidstat")

        # cache_hit_rate
        cache_hit_rate_sql = "select blks_hit / (blks_read + blks_hit + 0.001) " \
                             "from pg_stat_database " \
                             "where datname = '{}';".format(self.database)

        # 并发用户数量
        concurrent_users = """
        SELECT
            count(DISTINCT usename)
        AS
            concurrent_users
        FROM
            pg_stat_activity
        WHERE
            state = 'active';
        """

        # 锁等待次数
        lock = """
        SELECT
            count(*) AS lock_wait_count
        FROM
            pg_stat_activity
        WHERE waiting = true;
        """

        # 错误率
        error_rate = """
        SELECT
            (sum(xact_rollback) + sum(conflicts) + sum(deadlocks)) / (sum(xact_commit) + sum(xact_rollback) + sum(conflicts) + sum(deadlocks)) AS error_rate
        FROM
            pg_stat_database;
        """

        # 逻辑读/秒和物理读/秒
        read = """
        SELECT
            logical_reads / (extract(epoch from now() - stats_reset)) AS logical_reads_per_second,
            physical_reads / (extract(epoch from now() - stats_reset)) AS physical_reads_per_second
        FROM (
            SELECT
                sum(tup_returned + tup_fetched) AS logical_reads,
                sum(blks_read) AS physical_reads,
                max(stats_reset) AS stats_reset
            FROM
                pg_stat_database
            ) subquery;
        """

        # 活跃会话数量
        active_session = """
        SELECT
            count(*) AS active_session
        FROM
            pg_stat_activity;
        """

        # 每秒提交的事务数
        transactions_per_second = """
        SELECT
            total_commits / (extract(epoch from now() - max_stats_reset)) AS transactions_per_second
        FROM (
            SELECT
            sum(xact_commit) AS total_commits,
            max(stats_reset) AS max_stats_reset
        FROM
            pg_stat_database
            ) subquery;
        """

        # 扫描行、更新行和删除行
        tup = """
        SELECT
            rows_scanned / (extract(epoch from now() - max_stats_reset)) AS rows_scanned_per_second,
            rows_updated / (extract(epoch from now() - max_stats_reset)) AS rows_updated_per_second,
             rows_deleted / (extract(epoch from now() - max_stats_reset)) AS rows_deleted_per_second
        FROM (
            SELECT
            sum(tup_returned) AS rows_scanned,
            sum(tup_updated) AS rows_updated,
            sum(tup_deleted) AS rows_deleted,
            max(stats_reset) AS max_stats_reset
            FROM
             pg_stat_database
            ) subquery;
        """

        try:
            cursor.execute(cache_hit_rate_sql)
            result = cursor.fetchall()
            for s in result:
                state_list.append(float(s[0]))
            # print('cache_hit_rate_sql: ', state_list)

            # 并发用户数量
            cursor.execute(concurrent_users)
            result = cursor.fetchall()
            state_list.append(float(result[0][0]))
            # print('并发用户数量: ', state_list)

            # 锁等待次数
            # cursor.execute(lock)
            # result = cursor.fetchall()
            # state_list.append(float(result[0][0]))
            # print('锁等待次数: ', state_list)

            # 错误率
            cursor.execute(error_rate)
            result = cursor.fetchall()
            state_list.append(float(result[0][0]))
            # print('错误率: ', state_list)

            # 逻辑读和物理读
            cursor.execute(read)
            result = cursor.fetchall()
            # print(result)
            for i in result[0]:
                state_list.append(float(i))
            # print('逻辑读和物理读: ', state_list)

            # 活跃会话数
            cursor.execute(active_session)
            result = cursor.fetchall()
            # print(result)
            state_list.append(float(result[0][0]))
            # print('活跃会话: ', state_list)

            # 每秒提交的事务
            cursor.execute(transactions_per_second)
            result = cursor.fetchall()
            # print(result)
            state_list.append(float(result[0][0]))
            # print('每秒提交事务: ', state_list)

            # 扫描、更新、删除行
            cursor.execute(tup)
            result = cursor.fetchall()
            for i in result[0]:
                state_list.append(float(i))

            cursor.close()
            conn.close()
        except Exception as error:
            print(error)
        for i in range(len(state_list)):
            # print(i)
            state_list[i] = float(state_list[i])
        return state_list

    def change_knob(self, knobs):
        flag = True
        conn = self.get_conn()
        cursor = conn.cursor()
        reset_sql = """
        alter system set {}={};
        """
        for knob in knobs:
            # enum (include on/off)
            val = knobs[knob]
            # integer
            if self.knobs[knob]['type'] == 'integer':
                val = int(val)
            # real
            elif self.knobs[knob]['type'] == 'real':
                val = float(val)
            try:
                old_isolation_level = conn.isolation_level
                conn.set_isolation_level(0)
                cursor.execute(reset_sql.format(knob, val))
                conn.set_isolation_level(old_isolation_level)
            except Exception as error:
                print(error)
                flag = False
            # _, stdout, stderr = self.ssh.exec_command('gs_guc set -c "{}={}" -D {}'.format(knob, val, self.data_path))
            # info = stdout.read().decode('utf-8')
            # err = stderr.read().decode('utf-8')
            # if info.find("Success") == -1:
            #     flag = False
        _, stdout, stderr = self.ssh.exec_command(f'module load postgresql/12.2-gcc_13.1.0\npg_ctl stop -D {self.data_path}')
        info = stdout.read().decode('utf-8')
        # print(info)
        # print(stderr.read().decode('utf-8'))
        _, stdout, stderr = self.ssh.exec_command(f'module load postgresql/12.2-gcc_13.1.0\npg_ctl -D {self.data_path} -l logfile start')
        info = stdout.read().decode('utf-8')
        if 'server started' not in info:
            print(stderr.read().decode('utf-8'))
            flag = False
        
        _, stdout, stderr = self.ssh.exec_command(f'module load postgresql/12.2-gcc_13.1.0\npg_ctl reload -D {self.data_path}')
        info = stdout.read().decode('utf-8')
        if 'error' in info:
            print(stderr.read().decode('utf-8'))
            flag = False

        if flag:
            print('apply knobs successfully!')
        
        # _, stdout, stderr = self.ssh.exec_command('gs_om -t restart')
        # info = stdout.read().decode('utf-8')
        return flag
