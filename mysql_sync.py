import numpy as np
import pandas as pd
import json
import time
import datetime
import pymysql
from sshtunnel import SSHTunnelForwarder

import logging
from logging import handlers

class Logger(object):
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }#日志级别关系映射

    def __init__(self,filename,level='info',when='D',backCount=3, fmt='%(asctime)s %(levelname)s %(message)s %(filename)s[line:%(lineno)d]'):
        # fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        self.logger = logging.getLogger(filename)

        if not self.logger.handlers:
            format_str = logging.Formatter(fmt)#设置日志格式
            self.logger.setLevel(self.level_relations.get(level))#设置日志级别
            sh = logging.StreamHandler()#往屏幕上输出
            sh.setFormatter(format_str) #设置屏幕上显示的格式
            th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')#往文件里写入#指定间隔时间自动生成文件的处理器
            #实例化TimedRotatingFileHandler
            #interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
            # S 秒
            # M 分
            # H 小时、
            # D 天、
            # W 每星期（interval==0时代表星期一）
            # midnight 每天凌晨
            th.setFormatter(format_str)#设置文件里写入的格式
            self.logger.addHandler(sh) #把对象加到logger里
            self.logger.addHandler(th)

# if __name__ == '__main__':
#     log = Logger('all.log',level='debug')
#     log.logger.debug('debug')
#     log.logger.info('info')
#     log.logger.warning('警告')
#     log.logger.error('报错')
#     log.logger.critical('严重')
#     Logger('error.log', level='error').logger.error('error')


class MysqlSyncTool:

    def __init__(self):
        self.logger = Logger('./logs/all.log', level='debug').logger
        self.error_logger = Logger('./logs/error.log', level='error').logger


    def log(self, message, level='debug'):
        if level == 'debug':
            self.logger.debug(message)
        elif level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.error_logger.error(message)

    # 查询所有字段
    def list_col(self, db, table):
        #db = pymysql.connect(host, username, password, database, port=port, charset="utf8")
        cursor = db.cursor()
        cursor.execute("select * from %s limit 1" % table)
        col_name_list = [tuple[0] for tuple in cursor.description]
        return col_name_list

    # 列出所有的表
    def list_table(self, db):
        #db = pymysql.connect(host, username, password, database, port=port,  charset="utf8")
        cursor = db.cursor()
        cursor.execute("show tables")
        table_list = [tuple[0] for tuple in cursor.fetchall()]
        return table_list

    def db_create_mysql_sync_scheme_table(self, db, dest_metadata_table):
        tables = self.list_table(db)
        if dest_metadata_table in tables:
            #self.log("metadata表已存在")
            return True
        else:
            sql = """
                CREATE TABLE IF NOT EXISTS `mysql_sync_metadata` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `source_table` varchar(30) NOT NULL DEFAULT '',
                  `last_updated_at` int(11) NOT NULL DEFAULT '0',
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            #print(sql)
            try:
                # 执行sql语句
                cursor = db.cursor()
                cursor.execute(sql)
                self.log("创建metadata表 OK")
                return True
            except:
               # 发生错误时回滚
                db.rollback()
                self.log("创建metadata表失败", 'error')
                return False

    def db_insert(self, db, sql):
        try:
            # 执行sql语句
            cursor = db.cursor()
            cursor.execute(sql)
            # 执行sql语句
            db.commit()
            return True
        except:
           # 发生错误时回滚
            db.rollback()
            return False

    def db_update(self, db, sql):
        try:
            # 执行sql语句
            cursor = db.cursor()
            cursor.execute(sql)
            # 执行sql语句
            db.commit()
            return True
        except:
           # 发生错误时回滚
            db.rollback()
            return False

    def db_update_mysql_sync_scheme_time(self, db, table, source_table, updated_at):
        try:
            # 执行sql语句
            sql = "UPDATE {} SET last_updated_at={} WHERE source_table='{}'".format(table, updated_at, source_table)
            #print(sql)
            cursor = db.cursor()
            cursor.execute(sql)
            # 执行sql语句
            db.commit()
            return True
        except:
           # 发生错误时回滚
            db.rollback()
            return False

    def db_get_mysql_sync_scheme_time_or_set_0(self, db, dest_metadata_table, source_table):
        dest_cursor = db.cursor()
        sql = "SELECT {} FROM {} WHERE source_table='{}' LIMIT 1".format('*', dest_metadata_table, source_table)
        #print(sql)
        dest_cursor.execute(sql)
        result = dest_cursor.fetchone()
        metadata_update_index = 2 # HARD CODE
        if result != None:
            last_updated_at = result[metadata_update_index]
            return last_updated_at
        else:
             # 插入数据库，上次更新时间设置为0
            result = self.db_insert(db, "INSERT INTO {}(id, source_table, last_updated_at) VALUES (NULL, '{}', {})".format(dest_metadata_table, source_table, 0))
            if result == True:
                self.log('初始化配置时间成功')
            else:
                self.log('初始化配置时间失败', 'error')
                return False
            return 0

    def db_auto_create_table_if_not_exists(self, source_db, dest_db, source_table, dest_table, keep_cols):
        tables = self.list_table(dest_db)
        #print('tables:', tables)
        if dest_table in tables:
            # 目标表已经存在，不需要clone
            return True


        sql = "SHOW CREATE TABLE {}".format(source_table)
        source_cursor = source_db.cursor()
        source_cursor.execute(sql)
        result = source_cursor.fetchone()
        if result:
            source_scheme = result[1]
            self.log("源表scheme: {}".format(source_scheme))
            dest_scheme = source_scheme.replace("CREATE TABLE `{}`".format(source_table), "CREATE TABLE `{}`".format(dest_table))
            dest_cursor = dest_db.cursor()
            dest_cursor.execute(dest_scheme) # TODO 需要保证成功，如何获取结果
            self.log("创建目标表 {} OK".format(dest_table))

            if isinstance(keep_cols, str) and keep_cols == 'all':
                self.log("保留所有字段")
            elif isinstance(keep_cols, list) and len(keep_cols) > 0:
                self.log("保留字段：", keep_cols)
                cols = self.list_col(dest_db, dest_table)
                drop_cols = [col for col in cols if col not in keep_cols]
                #print("待删除字段：", drop_cols)
                for col in drop_cols:
                    self.db_drop_col(dest_db, dest_table, col)
            return True
        else:
            self.log("源表 {} 不存在".format(source_table), 'error')
            return False

    def db_drop_col(self, db, table, col):
        sql = "ALTER TABLE {} DROP COLUMN {}".format(table, col)
        cursor = db.cursor()
        cursor.execute(sql)
        return True

    def start_sync_task(self, configs, task, source_db, dest_db):
        # 检查是否需要clone目标表
        if task['auto_create'] == True:
            self.db_auto_create_table_if_not_exists(source_db, dest_db, task['source_table'], task['dest_table'], task['keep_cols'])

        # 暂停一下防止源数据库太繁忙
        if task['sync_page_sleep'] > 0:
            time.sleep(task['sync_page_sleep'])

        source_cursor = source_db.cursor()
        dest_cursor = dest_db.cursor()

        # 查询源表和目标表的所有列名
        source_cols = self.list_col(source_db, task['source_table'])
        #print('源表', task['source_table'])
        #print('源表列名', source_cols)
        dest_cols = self.list_col(dest_db, task['dest_table'])
        #print('目标表', task['dest_table'])
        #print('目标表列名', dest_cols)
        # 列名和row index转换
        source_col2index = {col:index for index,col in enumerate(source_cols)}
        source_index2col = {index:col for index,col in enumerate(source_cols)}
        dest_col2index = {col:index for index,col in enumerate(dest_cols)}
        dest_index2col = {index:col for index,col in enumerate(dest_cols)}

        # TODO:检查源表字段是否正确

        last_updated_at = self.db_get_mysql_sync_scheme_time_or_set_0(dest_db, configs['dest_metadata_table'], task['source_table'])
        self.log('上次更新时间 {}'.format(last_updated_at))

         # 读取最新修改的数据的总条数
        sql = "SELECT COUNT(*) FROM {} WHERE {}>={}".format(task['source_table'], task['update_col'], last_updated_at)
        source_cursor.execute(sql)
        result = source_cursor.fetchone()
        n_total = result[0] if result != None else 0
        self.log('n_total: {}'.format(n_total))
        if n_total <= 1:
            # 如果只有1条，说明是last_updated_at重复那条
            self.log('{}已经是最新了'.format(task['dest_table']))
            return True

        n_times = (n_total//task['sync_page_size']) + 1 # 保证即使只有1条也会更新
        self.log('n_times: {}'.format(n_times))

        for i in range(n_times):
            self.log('第{}次同步'.format(i))
            last_updated_at = self.db_get_mysql_sync_scheme_time_or_set_0(dest_db, configs['dest_metadata_table'], task['source_table'])
            self.log('上次更新时间 {}'.format(last_updated_at))
            # 读取最新修改的数据，按更新时间从旧到新排序，然后依次插入或更新目标数据库
            sql = "SELECT {} FROM {} WHERE {}>={} ORDER BY {} ASC LIMIT {}".format('*', task['source_table'], task['update_col'], last_updated_at, task['update_col'], task['sync_page_size']+1)
            #print(sql)
            source_cursor.execute(sql)
            results = source_cursor.fetchall()
            if results == None:
                self.log('没有读取到新数据，最新一条数据也没了，可能是源表把记录删除了')
                break
            if len(results) == 1:
                self.log('已经是最新了')
                break

            # 写入或更新目标数据库
            insert_count = 0
            update_count = 0
            for source_row in results:
                #print(result)
                # 获取源数据库中的该条记录的主键值
                primary_keys = task['primary_keys']
                source_primary_value_indexs = [source_col2index[primary_key] for primary_key in primary_keys]
                source_primary_values = [source_row[source_primary_value_index] for source_primary_value_index in source_primary_value_indexs]
                #print(source_primary_values) # 如果只有一个主键，则列表只有1个元素，如[5332]。如果有多个，则列表有多个元素[5332, 1124, ...]


                # 查询源主键值是否在目标表中
                primary_keys_where = " and ".join([str(primary_key)+'='+str(source_primary_values[i]) for i, primary_key in enumerate(primary_keys)])
                sql = "SELECT {} FROM {} WHERE {}".format('*', task['dest_table'], primary_keys_where)
                dest_cursor.execute(sql)
                result = dest_cursor.fetchone()
                #print(result)

                # 如果源主键值不在目标表中，则需要插入
                target_values_in_source = [source_row[source_col2index[col]] for col in dest_cols] # 从源表中获取需要插入目标表的字段
                # workround: mysql5.7.29以上DATE不能设置为0000-00-00。'0000-00-00'不是datetime.date
                target_values_in_source = ['1970-01-01' if value=='0000-00-00' else value for value in target_values_in_source]
                target_values_in_source = ['1970-01-01 00:00:00' if value=='0000-00-00 00:00' else value for value in target_values_in_source]
                # 其他日期是datetime.date类型，需要转化为字符串
                target_values_in_source = [str(value) if isinstance(value, datetime.date) else value for value in target_values_in_source]
                target_values_in_source = [str(value) if isinstance(value, datetime.datetime) else value for value in target_values_in_source]
                if result == None:
                    #print('target_values_in_source:')
                    #print(target_values_in_source)
                    #print(target_values_in_source) # [5332, '18482131983', '', 1578289786, 1578879378, 1, 3, 0, 2369, 1, 0, 1578289786, 1578289786]
                    target_values_str = ','.join(["'"+value+"'" if isinstance(value, str) else str(value) for value in target_values_in_source]) # 5332,'18482131983','',1578289786,1578879378,1,3,0,2369,1,0,1578289786,1578289786
                    #print(target_values_str)
                    sql = "INSERT INTO {} VALUES ({})".format(task['dest_table'], target_values_str)
                    #self.log(target_values_in_source)
                    #self.log(sql)

                    #print(sql)
                    result = self.db_insert(dest_db, sql)
                    if result == True:
                        #print('插入数据成功')
                        insert_count += 1
                        # 更新配置最新时间
                        updated_at = source_row[source_col2index[task['update_col']]]
                        result = self.db_update_mysql_sync_scheme_time(dest_db, configs['dest_metadata_table'], task['source_table'], updated_at)
                        if result == True:
                            #print('更新数据库配置时间成功')
                            pass
                        else:
                            self.log('更新数据库配置时间失败', 'error')
                    else:
                        #self.log(target_values_in_source)
                        self.log(sql)
                        self.log('插入数据失败', 'error')
                # 如果源主键值在目标表中，则需要更新
                elif result != None:
                    # 判断update字段是否相同
                    if source_row[source_col2index[task['update_col']]] == result[dest_col2index[task['update_col']]]:
                        #self.log('update字段相同，无需更新')
                        continue

                    # 如果不相同才更新
                    target_update_kv = [(dest_cols[i], "'"+value+"'" if isinstance(value, str) else str(value)) for i, value in enumerate(target_values_in_source)]
                    #print(target_update_kv)
                    target_update_str = ','.join([str(col) + '=' + value  for col, value in target_update_kv])
                    target_where_kv = [(primary_key, "'"+source_primary_values[i]+"'" if isinstance(source_primary_values[i], str) else str(source_primary_values[i])) for i, primary_key in enumerate(primary_keys)]
                    target_where_str = ','.join([str(col) + '=' + value  for col, value in target_where_kv])
                    #print(target_where_str)
                    sql = "UPDATE {} SET {} WHERE {}".format(task['dest_table'], target_update_str, target_where_str)
                    result = self.db_update(dest_db, sql)
                    if result == True:
                        #print('更新数据成功')
                        update_count += 1
                        # 更新配置最新时间
                        updated_at = source_row[source_col2index[task['update_col']]]
                        result = self.db_update_mysql_sync_scheme_time(dest_db, configs['dest_metadata_table'], task['source_table'], updated_at)
                        if result == True:
                            pass
                            #print('更新数据库配置时间成功')
                        else:
                            self.log('更新数据库配置时间失败', 'error')
                    else:
                        self.log('更新数据失败', 'error')
                # 如果主键只在目标中，表示需要删除（保留脏数据或者通过另外一个进程定时查询目标数据表中的所有主键，判断在不在源数据库中）
            self.log('INSERT {} 条，UPDATE {} 条'.format(insert_count, update_count))
        return True


    def start_sync(self):
        with open('mysql_sync.json', 'r') as f:
            configs = json.load(f)

            self.log('加载配置文件 OK')

            tasks = configs['tasks']
            # 打开源数据库
            source_db = pymysql.connect(configs['source_host'], configs['source_username'], configs['source_password'], configs['source_database'], port=configs['source_port'], charset="utf8")
            source_cursor = source_db.cursor()
            self.log('连接源数据库 OK')

            # 打开目标数据库
            dest_db = pymysql.connect(configs['dest_host'], configs['dest_username'], configs['dest_password'], configs['dest_database'], port=configs['dest_port'], charset="utf8")
            dest_cursor = dest_db.cursor()
            self.log('连接目标数据库 OK')

            # 初始化目标scheme表
            self.db_create_mysql_sync_scheme_table(dest_db, configs['dest_metadata_table'])

            for task in tasks:
                self.start_sync_task(configs, task, source_db, dest_db)

            source_db.close()
            dest_db.close()

            # 关闭打开的handler
            self.logger.handler = []
            self.error_logger.handler = []
            self.log('DONE')

if __name__ == "__main__":
    sync_tool = MysqlSyncTool()
    sync_tool.start_sync()