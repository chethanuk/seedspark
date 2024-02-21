import clickhouse_connect

client = clickhouse_connect.get_client(host="play.clickhouse.com", port=443, username="play")

uri = "clickhouse://demo:demo@github.demo.trial.altinity.cloud:8443/default?secure=True"
client = clickhouse_connect.get_client(
    host="github.demo.trial.altinity.cloud", port="8443", user="demo", password="demo", interface="https"
)
df = client.query_df("SELECT * FROM airports")
print(df.dtypes)
print(df.head())
