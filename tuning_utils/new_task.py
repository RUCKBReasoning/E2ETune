from new_task_base import *
import threading
from datetime import datetime


def generate_random_string(length=None):
    # chars = string.ascii_letters + string.digits
    # return ''.join(random.sample(chars, length))

    now = datetime.now()
    timestamp_str = now.strftime("%m%d_%H_%M_%S")
    return timestamp_str


class one_thread(threading.Thread):
    def __init__(self, workload_name):
        threading.Thread.__init__(self)
        self.id = generate_random_string(10)
        self.workload_name = workload_name
        self.schema_path = "workloads/" + workload_name + "_create" + ".sql"
        self.data_path = "workloads/" + workload_name + "_insert" + ".sql"
        self.wg_path = "workloads/" + workload_name + "_workload" + ".wg"
        self.log_path = "workloads/" + self.id + ".log"

    def run(self):
        connection, cur = connect_og(
            database_name="dwg",
            user_name="jikun",
            password="tianjikun123@",
            host="182.92.85.148",
            port=15400
        )
        print(connection.info)
        create_schema(connection,cur,self.schema_path)
        insert_data(connection, cur, self.data_path)
        # start_test(connection, cur, self.wg_path, self.log_path)


if __name__ == "__main__":
    thread1 = one_thread("sibench")
    # thread2 = one_thread("sibench")

    # 启动多个线程
    thread1.start()
    # thread2.start()

    # 等待所有线程完成
    thread1.join()
    # thread2.join()

    print("All threads have finished")
