# %%
import requests
import json
from msal import ConfidentialClientApplication
authorityBase = "https://login.microsoftonline.com/"
client_secret = 'uLk8Q~xxxxxxxxxxxxxxxxxxxxxxx'
client_id = '3xxxxxxxxxxxxxxxxxxxx'
tenant_id = 'axxxxxxxxxxxxxxxxxxxxxx'
SCOPES = ['User.Read']
authority = "https://login.microsoftonline.com/a56994bdxxxxxxxxxxxxx"


# %%
DATAVERSE_URL = "https://orgd2f85b65.api.crm.dynamics.com"
SCOPE = f"{DATAVERSE_URL}/.default"  # Dataverse API scope

# %%
# Dataverse table details
TABLE_NAME = 'jk_application' # Replace with the table logical name


# %%
# Authenticate and get an access token
def get_access_token(client_id, client_secret, tenant_id, scope):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )

    # Acquire token
    result = app.acquire_token_for_client(scopes=[scope])
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"Failed to acquire token: {result.get('error_description')}")

# %%
print("Authenticating...")
token = get_access_token(client_id, client_secret, tenant_id, SCOPE)
print("Authentication successful!")
print(token)

# %%
# Headers
headers = {
	"Authorization": f"Bearer {token}",
	"Accept": "application/json",
	"Content-Type": "application/json; charset=utf-8"
}

## column_lists
selected_columns = 'jk_name,jk_screeningcalldate,jk_offermade,jk_referencecheckrequired'

# %%
## Query Parameters.
params = {'$select': selected_columns}

# %%
endpoint_url = 'https://orgd2f85b65.crm.dynamics.com/api/data/v9.2/jk_applications'

# %%
response = requests.get(endpoint_url,headers=headers,params=params)
response.status_code

# %%
data = response.json()
data['value'][0]

# %%
len(data['value'])

# %%
## Creating the applications dataframe.
applications_list = []

for i in range(len(data['value'])):
	applications_dict = dict(
				jk_name = data['value'][i]['jk_name'],
				jk_screeningcalldate  = data['value'][i]['jk_screeningcalldate'],
				jk_offermade = data['value'][i]['jk_offermade'],
				jk_referencecheckrequired = data['value'][i]['jk_referencecheckrequired']
	                        )

	applications_list.append(applications_dict)



# %%
import pandas as pd
applications_df = pd.DataFrame(applications_list)
applications_df

# %%
## connecting to the sql server.

server = 'logicapps.database.windows.net'
username = 'xxxxxxxxxxxxx'
password = 'xxxxxx!'
database = 'bronze'
table_name = 'jk_applications'
schema_name = 'crm'



# %%
from sqlalchemy import create_engine

# %%
try:
    # Establish connection using SQLAlchemy and pyodbc
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)

    # Load DataFrame into the SQL Server table within the specified schema
    applications_df.to_sql(table_name, con=engine, schema=schema_name, if_exists='append', index=False)

    print(f"Data successfully loaded into table '{schema_name}.{table_name}' in the database '{database}'.")
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    # Dispose the engine to close connections
    engine.dispose()
