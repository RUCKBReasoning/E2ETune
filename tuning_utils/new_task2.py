from new_task import *


class one_thread_given_queries(threading.Thread):
    def __init__(self, wg, log_path, connection, cur, thread_id, time_stamp) -> None:
        threading.Thread.__init__(self)
        self.wg = wg
        self.log_path = log_path
        self.connection = connection
        self.cur = cur
        self.thread_id = thread_id
        self.time_stamp = time_stamp

    def run(self):
        try:
            sql_list = self.wg
            with open(self.log_path, 'w') as f:
                print(f"Thread {self.thread_id} log file start. Tot sql num : {len(sql_list)}")
                f.write(f"Thread {self.thread_id} log file start. Tot sql num : {len(sql_list)}\n")
                start_time = time.time()
                for i, it in enumerate(sql_list):
                    # print(it)
                    error_info = it
                    self.cur.execute(it)
                    self.connection.commit()
                    # print(f"Thread {self.thread_id} sql id {i}")
                    # if i % 200 == 0:
                    f.write(f"Thread {self.thread_id} {i} sqls has been processed successfully.\n")
                    print(f"Thread {self.thread_id} {i} sqls has been processed successfully.\n")
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    f.write(f"Thread {self.thread_id} processed time: {elapsed_time} seconds.\n")
                    print(f"Thread {self.thread_id} processed time: {elapsed_time} seconds.\n")

                end_time = time.time()
                self.time_stamp[self.thread_id] = key(len(sql_list), end_time - start_time)

        except Exception as e:
            print("Error: ", e)
            print('error_info:', error_info)


class multi_thread:
    def __init__(self, db, workload_path, thread_num, log_path):
        self.wg_file = None
        self.id = generate_random_string(10)
        self.workload_name = workload_path
        self.thread_num = thread_num
        self.schema_path = "workloads/" + workload_path + "_create" + ".sql"
        self.data_path = "workloads/" + workload_path + "_insert" + ".sql"
        # self.wg_path = "workloads/" + workload_path + "_workload" + ".wg"
        self.db = db
        self.wg_path = workload_path
        self.log_path = log_path
        self.sql_list_idx = dict()

    def data_pre(self):
        connection, cur = connect_og(
            database_name=self.db.database,
            user_name=self.db.user,
            password=self.db.password,
            host=self.db.host,
            port=self.db.port
        )
        # create_schema(connection, cur, self.schema_path)
        # insert_data(connection, cur, self.data_path)
        connection.close()

        with open(self.wg_path, 'r') as f:
            self.wg_file = f.read()

        sql_list = re.split(r'[;\n]+', self.wg_file)
        for i, it in enumerate(sql_list):
            sql_list[i] += ";"

        if sql_list[-1] == ";":
            sql_list = sql_list[0:-1]

        if len(sql_list) > 3000:
            sql_list = sql_list[:3000]

        self.sql_list_idx = dict()

        for i in range(self.thread_num):
            self.sql_list_idx[i] = []

        for i in range(len(sql_list)):
            self.sql_list_idx[i % self.thread_num].append(sql_list[i])

    def run(self):
        connection, cur = connect_og(
            database_name=self.db.database,
            user_name=self.db.user,
            password=self.db.password,
            host=self.db.host,
            port=self.db.port
        )
        threads = []
        time_stamp = dict()

        for i in range(self.thread_num):
            thread = one_thread_given_queries(
                wg=self.sql_list_idx[i],
                log_path=self.log_path,
                connection=connection,
                cur=cur,
                thread_id=i,
                time_stamp=time_stamp
            )
            threads.append(thread)

        start_time = time.time()
        for it in threads:
            it.start()
        for it in threads:
            it.join()
        end_time = time.time()

        with open(self.log_path, 'w') as f:
            f.write(f"total sql num : {len(self.wg_file)}\n")
            f.write(f"total time consumed : {end_time - start_time}\n")
            for i in range(self.thread_num):
                f.write(f"\tthread {i} processed sql num : {time_stamp[i].value}\n")
                f.write(f"\tthread {i} using time : {time_stamp[i].type}\n")
        connection.close()
        print('length of sql list: ',len(self.sql_list_idx[0]))
        print('total time: ',end_time - start_time)
        return [ -(end_time - start_time) / (len(self.sql_list_idx[0]) * self.thread_num),\
                len(self.sql_list_idx[0]) / (end_time - start_time) * self.thread_num]
        # return


# if __name__ == "__main__":
#     mh = multi_thread("sibench", 10)
#     mh.data_pre()
#     mh.run()
#     print("All threads have finished")
