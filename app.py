import new_IAM_role, new_Redshift_Cluster, redshift_cluster_actions


def print_instructions():
    """
    Default options of the CLI
    """
    print("Options: ")
    print("1 - Create new IAM role and attach it")
    print("2 - Create a new Redshift Cluster")
    print("3 - Check cluster status")
    print("4 - Open TCP Connection")
    print("5 - Connect to cluster")
    print("6 - Create tables")
    print("7 - Verify tables")
    print("8 - Delete Cluster")
    print("9 - Delete IAM role")
    print("e - Exit program")


def main():
    print_instructions()
    user_input = input("Enter your option: ")
    while user_input != "e":
        if str(user_input) == "1":
            new_IAM_role.main()

        if str(user_input) == "2":
            new_Redshift_Cluster.main()

        if str(user_input) == "3":
            redshift_cluster_actions.cluster_status()

        if str(user_input) == "4":
            redshift_cluster_actions.open_TCPConnection_to_acceces_cluster()

        if str(user_input) == "5":
            redshift_cluster_actions.connect_cluster()

        if str(user_input) == "6":
            redshift_cluster_actions.create_tables()

        if str(user_input) == "7":
            redshift_cluster_actions.verify_tables()

        if str(user_input) == "8":
            redshift_cluster_actions.delete_cluster()

        if str(user_input) == "9":
            redshift_cluster_actions.deatach_and_delete_IAM_role()

        print_instructions()
        user_input = input("Enter your option: ")


if __name__ == "__main__":
    main()
