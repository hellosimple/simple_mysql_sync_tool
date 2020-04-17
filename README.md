### MYSQL增量同步工具
市面上的同步软件较复杂，安装配置都较麻烦，我想简单的同步一下mysql做一些离线计算，所以自己实现了一个。
如果基于binlog的同步虽然可靠，但是我只想同步几个表也会读取所有binlog，占用资源和耗时。

使用场景：想做离线数据分析，但是又不想占用业务数据库过多带宽，所以同步一份到本地进行分析。

### 同步原理
利用配置的更新字段，记录每个表最后更新时间，后面查询的时候查>=最后更新时间的数据，实现增量同步。

限制：目前无法同步删除表。

### 支持功能
1. 支持根据源表的【更新时间】字段，增量同步表
2. 支持同步全部字段或部分字段
3. 支持自动创建目标表，并自动删除保留字段外的字段
4. 支持文件log和错误log打印

### 必要前提
依赖更新字段。源表必须要有一个int类型的创建时间或更新时间（目前我是用的11位时间戳）


### 使用步骤
```
pip3 install pymysql
git clone git@github.com:hellosimple/simple_mysql_sync_tool.git
cd simple_mysql_sync_tool
mv mysql_sync.json mysql_sync_local.json
配置源数据库和目标数据库，配置需要同步的表和字段（注意目标数据库为空就行，scheme表会自动创建）
python mysql_sync.py 首次运行全量同步，以后再运行就是增量同步
```

### 配置说明
```
{
    "source_host": "源数据库IP",
    "source_username": "源 Mysql 用户名",
    "source_password": "源 Mysql 密码",
    "source_database": "源数据库名",
    "source_port": 3306,
    "dest_host": "目标数据库IP",
    "dest_username": "目标 Mysql 用户名",
    "dest_password": "目标 Mysql 密码",
    "dest_database": "目标数据库名",
    "dest_port": 3306,
    "dest_metadata_table": "mysql_sync_metadata",
    "tasks": [
        # 同步表1，自动创建目标表并只保留需要的字段
        {
            "source_table": "table_user",
            "dest_table": "table_user_ori",
            "primary_keys": ["id"],
            "update_col": "updated_at",
            "sync_page_size": 20,
            "sync_page_sleep": 0.1, 
            "auto_create": true,
            "keep_cols": [
                "id",
                "user",
                "nickname",
                "created_at",
                "updated_at"
             ]
        },
        # 同步表2，直接完全复制目标表
        {
            "source_table": "table_log",
            "dest_table": "table_log_ori",
            "primary_keys": ["id"],
            "update_col": "created_at",
            "sync_page_size": 100,
            "sync_page_sleep": 0.1, 
            "auto_create": true,
            "keep_cols": "all"
        }
    ]
}
```

```
source_database：源数据库
dest_database：目标数据库
dest_metadata_table：用于存储最近更新时间的元数据，自动在目标数据库中建表

task:
    source_table：源表名
    dest_table: 目标表名
    primary_keys：指定源表的主键（支持多个主键），用于数据的更新
    update_field：每个表都需要一个代表数据更新的字段，用于判断增量更新
    sync_page_size：每次同步行数，主要用来避免初次更新的行数太多
    sync_page_sleep：每次同步休眠的时间（单位秒），避免业务数据库繁忙。如不需要休眠，可以设置为0。
    auto_create：自动创建表（当目标表不存在时）。
                如果设置为true，则目标表会自动创建，且和源表结构一致。
                如果设置为false，则目标表需要手动创建（可以和源表一样或者是源表的子集），但是目标表的字段名需要和源表一致。
    keep_cols：保留字段列表，仅在克隆模式为true、首次创建时生效。如果需要保留所有字段，则设置为字符串 "all"
```


