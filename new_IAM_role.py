from botocore.exceptions import ClientError
import boto3
import configparser
import json


def create_IAM_Role(iam, DWH_IAM_ROLE_NAME):
    try:
        print("   1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print("       " + str(e))

    print("   1.2 Attaching Policy")

    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("   1.3 Getting the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    print("       IAM role ARN: " + roleArn)
    return roleArn


def main():
    config = configparser.ConfigParser()

    config.read_file(open("dwh.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )

    print("----------------------------------------------------------------")
    print("1 - Creating IAM_Role")
    create_IAM_Role(iam, DWH_IAM_ROLE_NAME)
    print("----------------------------------------------------------------")


if __name__ == "__main__":
    main()
