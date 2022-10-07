import configparser
import psycopg2
import pandas as pd


def get_number_records_query(table_name):
    return "SELECT COUNT(*) FROM " + table_name


def get_column_attributes_from_table_query(table_name):
    return f"""
      SELECT "column", type, encoding, distkey, sortkey
      FROM pg_table_def
      WHERE TABLENAME = '{table_name}'
  """


def get_number_records(cur):
    tables_name = [
        "staging_events",
        "staging_songs",
        "songplays",
        "users",
        "songs",
        "artists",
        "time",
    ]

    for table_name in tables_name:
        cur.execute(get_number_records_query(table_name))
        results = cur.fetchone()

        for n in results:
            print("--------------------------------------------------------")
            print(f"Table '{table_name}' has {n} records")
            cur.execute(get_column_attributes_from_table_query(table_name))
            columns_descriptions = cur.fetchall()

            columns_names = [description[0] for description in columns_descriptions]
            columns_types = [description[1] for description in columns_descriptions]

            print(
                pd.DataFrame(
                    {
                        "column_name": columns_names,
                        "column_type": columns_types,
                    }
                )
            )


def main():
    """
    Function to verify the tables rows and columns.
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    get_number_records(cur)
    conn.close()


if __name__ == "__main__":
    main()
