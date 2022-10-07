from botocore.exceptions import ClientError
import boto3
import configparser
import pandas as pd
import redshift_cluster_actions
import time


def create_redshift_cluster(redshift, roleArn, **configs):
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=configs["DWH_CLUSTER_TYPE"],
            NodeType=configs["DWH_NODE_TYPE"],
            NumberOfNodes=int(configs["DWH_NUM_NODES"]),
            # Identifiers & Credentials
            DBName=configs["DWH_DB"],
            ClusterIdentifier=configs["DWH_CLUSTER_IDENTIFIER"],
            MasterUsername=configs["DWH_DB_USER"],
            MasterUserPassword=configs["DWH_DB_PASSWORD"],
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )
        return response["Cluster"]
    except Exception as e:
        print(e)


def main():
    config = configparser.ConfigParser()

    config.read_file(open("dwh.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    REGION = config.get("AWS", "REGION")

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    print("----------------------------------------------------------------")
    print("Clusters Configuration")
    print(
        pd.DataFrame(
            {
                "Param": [
                    "DWH_CLUSTER_TYPE",
                    "DWH_NUM_NODES",
                    "DWH_NODE_TYPE",
                    "DWH_CLUSTER_IDENTIFIER",
                    "DWH_DB",
                    "DWH_DB_USER",
                    "DWH_DB_PASSWORD",
                    "DWH_IAM_ROLE_NAME",
                ],
                "Value": [
                    DWH_CLUSTER_TYPE,
                    DWH_NUM_NODES,
                    DWH_NODE_TYPE,
                    DWH_CLUSTER_IDENTIFIER,
                    DWH_DB,
                    DWH_DB_USER,
                    DWH_DB_PASSWORD,
                    DWH_IAM_ROLE_NAME,
                ],
            }
        )
    )
    print("----------------------------------------------------------------")
    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION,
    )

    redshift = boto3.client(
        "redshift",
        region_name=REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    print("Role ARN:  " + roleArn)
    print("----------------------------------------------------------------")
    print("2 - Creating Redshift_cluster")
    created_cluster = create_redshift_cluster(
        redshift,
        roleArn,
        DWH_CLUSTER_TYPE=DWH_CLUSTER_TYPE,
        DWH_NUM_NODES=DWH_NUM_NODES,
        DWH_NODE_TYPE=DWH_NODE_TYPE,
        DWH_CLUSTER_IDENTIFIER=DWH_CLUSTER_IDENTIFIER,
        DWH_DB=DWH_DB,
        DWH_DB_USER=DWH_DB_USER,
        DWH_DB_PASSWORD=DWH_DB_PASSWORD,
    )
    print("    2.1 - Created Cluster:")
    print(redshift_cluster_actions.prettyRedshiftProps(created_cluster))
    print("    2.2 - Cluster status:")
    print(
        "    (A verfication will be done every 30 seconds untill the cluster is available)"
    )
    status = "creating"
    while status != "available":
        print("status:")
        status = redshift_cluster_actions.get_cluster_status()
        print("     " + redshift_cluster_actions.get_cluster_status())
        time.sleep(30)
    print("----------------------------------------------------------------")


if __name__ == "__main__":
    main()
