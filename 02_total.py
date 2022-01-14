from pyspark.sql import SparkSession
import pyspark.sql.functions as f


# Creates spark session with jdbc for SQL Server
spark = SparkSession\
    .builder\
    .master('local[*]')\
    .appName('Connection-Test')\
    .config('spark.driver.extraClassPath', 'C:/Users/marci/Documents/jdbc/sqljdbc_6.0/enu/jre8/sqljdbc42.jar')\
    .config('spark.executor.extraClassPath', 'C:/Users/marci/Documents/jdbc/sqljdbc_6.0/enu/jre8/sqljdbc42.jar')\
    .getOrCreate()


# Creates connection variables
server_name = "jdbc:sqlserver://127.0.0.1:50665"
database_name = "desafio_engenheiro"
url = server_name + ";" + "databaseName=" + database_name + ";"
table_name = "cliente"
user = "marcio"
password = "123456"

# Queries to load the target tables
qry_cliente = """ (
    SELECT *
    FROM cliente
    ) t """

qry_contrato = """ (
    SELECT *
    FROM contrato
    ) t """

qry_transacao = """ (
    SELECT *
    FROM transacao
    ) t """



# Function to load the DFs from SQL
def read_df(url, query_str, user, password):
    df = spark.read.format('jdbc')\
        .option('url',url)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', query_str)\
        .option("user", user) \
        .option("password", password) \
        .load()
    
    return df


# Loads the DFs
df_contrato = read_df(url,qry_contrato,user,password)
df_transacao = read_df(url,qry_transacao,user,password)


# Merges df_contrato and df_transacao, drops any contracts that aren't currently active and fills the null values with 0 so we can use math on those collumns
df = df_transacao.join(df_contrato, df_transacao.contrato_id == df_contrato.contrato_id)\
                .where(f.col("ativo").like('true'))
df = df.na.fill(value=0)

df = df.withColumn('total', (df.valor_total*(1-(df.percentual_desconto/100))))


df.agg({'total':'sum'}).show()