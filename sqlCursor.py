import pymysql
from common.log import logger


class SqlCursor:
    def __init__(self, host:str="localhost", user:str="root", passwd:str="123456", dbName:str="db", tbName:str="tb", port=3306, config=None) -> None:
        self.host = config['host'] if config else host
        self.user = config['user'] if config else user
        self.passwd = config['passwd'] if config else passwd
        self.dbName = config['dbName'] if config else dbName
        self.tbName = config['tbName'] if config else tbName
        self.port = config['port'] if config else port

    # 连接数据库
    def connection(self):
        try:
            connection = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database=self.dbName, port=self.port)
            return connection
        except Exception as e:
            logger.error(f"[MysqlKeyword Cursor] connect fail {e}")
            return None
      
    # 检查数据库是否存在，如果不存在则创建
    def create_database(self):
        try:
            connection = pymysql.connect(host=self.host, user=self.user, password=self.passwd)
        except Exception as e:
            logger.error(f"[MysqlKeyword Cursor] connect db fail {e}")
            return False
        if connection:
            with connection.cursor() as cursor:
                sql = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{self.dbName}'"
                cursor.execute(sql)
                result = cursor.fetchone()
                if result is None:
                    sql = f"CREATE DATABASE {self.dbName}"
                    cursor.execute(sql)
                    logger.info(f"Database '{self.dbName}' created successfully.")
                    return True
                else:
                    # logger.info(f"Database '{self.dbName}' already exists.")
                    return True
        else:
            return False
  
    # 检查数据表是否存在，如果不存在则创建表结构
    def create_table(self):        
        sql = f"SHOW TABLES LIKE '{self.tbName}'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                res = cursor.fetchone()
                if not res:
                    create_sql = f"CREATE TABLE {self.tbName} (id INT AUTO_INCREMENT PRIMARY KEY, keyword VARCHAR(255), response TEXT, state VARCHAR(20) DEFAULT 'active', create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                    cursor.execute(create_sql)
                    connection.commit()
                    logger.info("[MysqlKeyword Cursor] table created")
                    return True
                else:
                    return True
        else:
            return False

    # 插入数据
    def insert_data(self, keyword, response):
        if not self.validInput(keyword, response):
            return False
        logger.debug(f"[MysqlKeyword Cursor] insert_data {keyword}, {response}")
        sql = f"INSERT INTO {self.tbName} (keyword, response) VALUES (%s, %s)"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (keyword, response))
                connection.commit()
                logger.info("[MysqlKeyword Cursor] insert success")
                return True
        else:
            return False
      
    # 更新关键词状态
    def set_key_state(self, keyword, response=None, state="active"):
        if not self.validInput(keyword, response):
            return False
        logger.debug(f"[MysqlKeyword Cursor] update_data {keyword}, {state}")
        connection = self.connection()
        if response is None:
            sql = f"UPDATE {self.tbName} set state='%s' where keyword='%s'"
        else:
            sql = f"UPDATE {self.tbName} set state='%s' where keyword='%s' and response='%s'"
        if connection:
            with connection.cursor() as cursor:
                if response is None:
                    res = cursor.execute(sql, (state, keyword))
                else:
                    res = cursor.execute(sql, (state, keyword, response))
                connection.commit()
                logger.debug(f"[MysqlKeyword Cursor] update success {res}")
                return res
        else:
            return False
   
     # 更新数据
    def update_data(self, keyword, newResponse):
        if not self.validInput(keyword, newResponse):
            return False
        logger.debug(f"[MysqlKeyword Cursor] update_data {keyword}, {newResponse}")
        sql = f"UPDATE {self.tbName} set response='%s' where keyword='%s'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                res = cursor.execute(sql, (keyword, newResponse))
                connection.commit()
                logger.info(f"[MysqlKeyword Cursor] update success {res}")
                return res
        else:
            return False
      
    # 插入需求数据
    def require_data(self, keyword):
        if not self.validInput(keyword):
            return False
        logger.debug(f"[MysqlKeyword Cursor] require_data {keyword}")
        sql = f"INSERT INTO {self.tbName} (keyword, state) VALUES (%s, %s)"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (keyword, "require"))
                connection.commit()
                logger.info("[MysqlKeyword Cursor] insert success")
                return True
        else:
            return False
      
    # 删除数据
    def remove_data_all(self, keyword):
        if not self.validInput(keyword):
            return False
        logger.debug(f"[MysqlKeyword Cursor] remove_data_all {keyword}")
        sql = f"DELETE FROM {self.tbName} where keyword='%s'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                res = cursor.execute(sql, (keyword))
                connection.commit()
                logger.info(f"[MysqlKeyword Cursor] remove all success {res}")
                return res
        else:
            return False
      
    # 删除数据
    def remove_key(self, keyword:str):
        if not self.validInput(keyword):
            return False
        logger.debug(f"[MysqlKeyword Cursor] remove_key {keyword}")
        sql = f"DELETE FROM {self.tbName} where keyword='%s'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                res = cursor.execute(sql, (keyword))
                if res>1:
                    connection.rollback()
                else:
                    connection.commit()
                logger.info(f"[MysqlKeyword Cursor] remove success {res}")
                return res
        else:
            return False
      
    # 删除指定数据
    def remove_key_value(self, keyword, response):
        if not self.validInput(keyword, response):
            return False
        logger.debug(f"[MysqlKeyword Cursor] remove_key_value {keyword}, {response}")
        sql = f"DELETE FROM {self.tbName} where keyword='%s' and response='%s'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                res = cursor.execute(sql, (keyword, response))
                if res>1:
                    connection.rollback()
                else:
                    connection.commit()
                logger.info(f"[MysqlKeyword Cursor] remove key response success {res}")
                return res
        else:
            return False
    # 查询数据
    def query_data(self):
        logger.debug("[MysqlKeyword Cursor] query_data")
        sql = f"SELECT * FROM {self.tbName}"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
        else:
            return False

    # 查询全部键
    def query_keys(self):
        logger.debug("[MysqlKeyword Cursor] query_keys")
        sql = f"SELECT keyword FROM {self.tbName} where state='active'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
          
    # 查询全部需求键值
    def query_requirements(self):
        logger.debug("[MysqlKeyword Cursor] query_keys")
        sql = f"SELECT keyword FROM {self.tbName} where state='require'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
          
    # 模糊查找数据
    def search_data(self, keyword):
        if not self.validInput(keyword):
            return False
        logger.debug(f"[MysqlKeyword Cursor] search_data {keyword}")
        sql = f"SELECT keyword,response FROM {self.tbName} WHERE keyword LIKE %s and state='active'"
        connection = self.connection()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (f"%{keyword}%"))  # 这里将like_key作为参数传递，避免SQL注入攻击
                result = cursor.fetchall()
                if result:
                    return result  # 返回匹配的response值
                else:
                    return None  # 如果没有匹配的记录，返回None


    # 连接数据库并检查数据表是否存在
    def checkDBTB(self):
        if self.create_database():
            return self.create_table()
        else:
            return False
        
    def validInput(self, *texts)->bool:
        for text in texts:
            if text and len(text.strip())==0:
                return False
        return True

# 测试函数
def test():
    cur = SqlCursor("localhost", "root", "1231123", "tb_test")
    # if not cur.table_exists('keywords'):  # 检查数据表是否存在，如果不存在则创建表结构
    #     cur.create_table()
    cur.insert_data('test_key', 'test_response')  # 插入数据
    # cur.query_data()  # 查询数据
    # cur.search_data('test')  # 模糊查找数据，关键词为'te'的匹配项将会被查询出来
