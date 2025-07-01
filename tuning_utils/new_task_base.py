import psycopg2
import time
import re
from schema_alter import *
import json


def connect_og(database_name, user_name, password, host, port):
    connection = psycopg2.connect(database=database_name,
                                  user=user_name,
                                  password=password,
                                  host=host,
                                  port=port)
    cur = connection.cursor()
    return connection, cur


def create_schema(connection, cur, schema_path):
    with open(schema_path, 'r') as f:
        file_contents = f.read()
    cur.execute(file_contents)
    connection.commit()
    print("create schema...")


def insert_data(connection, cur, insert_path):
    with open(insert_path, 'r') as f:
        file_contents = f.readlines()
        for i, l in enumerate(file_contents):
            if i % 100 == 0:
                print('insert: {}'.format(i + 1))
            cur.execute(l)
    connection.commit()
    print("insert data...")


def start_test(connection, cur, wg_path, log_path):
    try:
        error_info = []

        with open(wg_path, 'r') as f:
            wg_file = f.read()

        sql_list = re.split(r'[;\n]+', wg_file)
        for i, it in enumerate(sql_list):
            sql_list[i] += ";"

        if sql_list[-1] == ";":
            sql_list = sql_list[0:-1]

        with open(log_path, 'w') as f:
            f.write("log file start.\n")
            start_time = time.time()
            for i, it in enumerate(sql_list):
                error_info = it
                cur.execute(it)
                connection.commit()
                if (i + 1) % 50 == 0:
                    f.write(f"{i + 1} sqls has been processed successfully.\n")
                    print(f"{i + 1} sqls has been processed successfully.")
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    f.write(f"processed time: {elapsed_time} seconds.\n")
                    print(f"processed time: {elapsed_time} seconds.")

    except Exception as e:
        print("Error: ", e)
        print(error_info)


class jsonParser:
    def __init__(self) -> None:
        self.dbs = None
        self.create_sql = []

    def parse_schema(self, schema_path):
        jsonFile = open(schema_path, 'r')
        input = json.loads(jsonFile.read())
        # print(input)
        # 存放表
        all_tables = []
        # 存放约束
        cons = []
        foreign_cons = []
        # 加载表名和列名
        # 循环读入各表信息
        for table in input['Tables']:
            tb_name = table['Table Name']
            tb_col_distribution = table['Column Distribution']
            tb_cols = []
            for col in table['Table Columns']:
                col_name = col['Column Name']
                col_type = col['Data Type']
                tb_cols.append(Column(col_name, col_type))

            prim_key = key(table['Primary Key']['Name'], table['Primary Key']['Data Type'])
            for con in table['Foreign Key']:
                foreign_cons.append(foreign_constraint(tb_name, key(con['Foreign Key Name'], con['Foreign Key Type']),
                                                       con['Referenced Table'], key(con['Referenced Primary Key'], con[
                        'Referenced Primary Key Type'])))
            all_tables.append(Table(tb_name, tb_cols, prim_key, foreign_cons, tb_col_distribution))

        self.dbs = DBschema(tbs=all_tables, foreign_constraint=foreign_cons)
        return

    def json2createSQL(self, sql_path) -> None:
        if self.dbs is None:
            print("error: schema not parsed correctly.")
        tb_num = len(self.dbs.tables)
        for i in range(tb_num):
            one_sql = simpleSQL()
            one_sql.add(key("create", "keyword"))
            one_sql.add(key("table", "keyword"))
            one_sql.add(key(self.dbs.tables[i].name, "tbname"))

            attr = "("
            len_ = len(self.dbs.tables[i].col)
            for i, it in enumerate(self.dbs.tables[i].col):
                attr += it.name
                attr += " "
                attr += it.data_type
                if i != len_ - 1:
                    attr += ","
                else:
                    attr += ")"

            one_sql.add(key(attr, "attrname"))
            one_sql.add(key(";", "end"))
            self.create_sql.append(one_sql)

            with open(sql_path, 'w') as f:
                for sql in self.create_sql:
                    f.write(sql.toStr())


# p = jsonParser()
# p.parse_schema('../configure/input2.json')
# p.json2createSQL('./workloads/sibench_create.sql')
