# table_name = "integers"

# psql_jdbc = "jdbc:postgresql://demoteddy-5340.6zw.aws-eu-west-1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
# connection_properties = {
#     # Postgres
#     "driver": "org.postgresql.Driver",
#     "user": "demo",
#     "password": "fdVWHGGDfRzS2J3tjr2GCA",
#     "ssl": str(self.clickhouse_config.ssl)
#     # "sslmode": "strict",  # Can be 'strict', 'none', etc.,
# }

# postgres = Postgres(
#     host="demoteddy-5340.6zw.aws-eu-west-1.cockroachlabs.cloud",
#     user="demo",
#     port=26257,
#     password="fdVWHGGDfRzS2J3tjr2GCA",
#     database="defaultdb",
#     extra={"ssl": "true", "ssl" :"verify-full"},
#     spark=self.spark,
# )
# print(f"postgres: {postgres.jdbc_url}")
# reader = DBReader(
#     connection=postgres,
#     source="public.table_tennis_players_rank",
#     columns=["country", "player_name"],
#     # hwm=DBReader.AutoDetectHWM(name="psome_hwm_name", expression="id"),
# )
# with SnapshotStrategy():
#     df = reader.run()
#     df.show()
#     df.printSchema()

# # self.spark.read.jdbc(url=psql_jdbc, table="table_tennis_players_rank", properties=connection_properties).show()
