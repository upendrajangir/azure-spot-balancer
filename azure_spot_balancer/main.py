from db.db_utils import create_tables


def main():
    create_tables()
    # Other setup and main loop code goes here

if __name__ == '__main__':
    #Define variables
    subscription_id = "f1b3c4d5-6e7f-8a9b-0c1d2e3f4a5b"
    resource_group = "my_resource_group"
    cluster_name = "my_cluster"
    spotpools = ["spotpool1", "spotpool2"]
    ondemandpools = ["ondemandpool1", "ondemandpool2"]





    main()
