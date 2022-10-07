import pandas as pd
import boto3
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
REGION = config.get("AWS", "REGION")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

redshift = boto3.client(
    "redshift",
    region_name=REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

ec2 = boto3.resource(
    "ec2", region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET
)

iam = boto3.client(
    "iam",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    region_name=REGION,
)


def prettyRedshiftProps(props):
    pd.set_option("display.max_colwidth", None)
    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def get_DWH_ENDPOINT():
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    return myClusterProps["Endpoint"]["Address"]


def get_cluster_status():
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    return myClusterProps["ClusterStatus"]


def cluster_status():
    print("----------------------------------------------------------------")
    print(" 3 - Listing Status:")
    try:
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        print(prettyRedshiftProps(myClusterProps))
        DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
        DWH_ROLE_ARN = myClusterProps["IamRoles"][0]["IamRoleArn"]
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
        print("----------------------------------------------------------------")
    except Exception as e:
        print(e)
        print("----------------------------------------------------------------")


def open_TCPConnection_to_acceces_cluster():
    try:
        print("----------------------------------------------------------------")
        print("4 - Security group:")
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        vpc = ec2.Vpc(id=myClusterProps["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        response = defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
        print(response)
    except Exception as e:
        print(e)
        print("----------------------------------------------------------------")


def connect_cluster():
    try:
        print("----------------------------------------------------------------")
        print("Trying to connect...")
        DWH_ENDPOINT = get_DWH_ENDPOINT()
        conn = psycopg2.connect(
            f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}"
        )
        cur = conn.cursor()
        print("Successfull connection!")
        conn.close()
        print("----------------------------------------------------------------")
    except Exception as e:
        print(e)
        print("----------------------------------------------------------------")


def delete_cluster():
    try:
        print("----------------------------------------------------------------")
        print("8 - Deleting Cluster")
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
        )
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        print(prettyRedshiftProps(myClusterProps))
    except Exception as e:
        print(e)
        print("----------------------------------------------------------------")
        ## check if is deleted (usar o while lá)


def deatach_and_delete_IAM_role():
    try:
        print("9.1 - Detaching IAM role...")
        detach = iam.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
        print(detach)
        print("9.2 - Deleting IAM role...")
        delete = iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        print(delete)
        print("----------------------------------------------------------------")
    except Exception as e:
        print(e)
        print("----------------------------------------------------------------")


def create_tables():
    try:
        print("----------------------------------------------------------------")
        print("Trying to connect...")
        DWH_ENDPOINT = get_DWH_ENDPOINT()
        conn = psycopg2.connect(
            f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}"
        )
        cur = conn.cursor()
        print("Successfull connection!")
        cur.execute(open("create_tables.sql", "r").read())
        print("Tables created successfully")
        conn.commit()
        conn.close()
        print("----------------------------------------------------------------")
    except Exception as e:
        print(e)
        print("----------------------------------------------------------------")


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
            print("--------------------------------------------------------")


def verify_tables():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    DWH_ENDPOINT = get_DWH_ENDPOINT()
    conn = psycopg2.connect(
        f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}"
    )
    cur = conn.cursor()

    get_number_records(cur)
    conn.close()
