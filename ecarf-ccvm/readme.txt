https://developers.google.com/datastore/docs/activate

gcutil --project <PROJECT_ID> addinstance <INSTANCE_NAME> --service_account_scopes=\
https://www.googleapis.com/auth/userinfo.email,\
https://www.googleapis.com/auth/datastore
# replace <PROJECT_ID> with the Project ID you created previously.
# replace <INSTANCE_NAME> with the name you want to use for your instance.