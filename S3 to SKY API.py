import json
import boto3
import requests
from urllib.parse import urlencode
import pandas as pd


# Define a function to retrieve JSON content from an S3 bucket
def retrieve_json_from_s3(bucket_name, json_file_name):
    """
    :param str bucket_name: Name of the S3 bucket where the JSON file is stored.
    :param str json_file_name: Name of the JSON file to retrieve.
    :return: Parsed JSON data.
    :rtype: dict

    This method retrieves a JSON file from an S3 bucket and returns the parsed JSON data.

    The `bucket_name` parameter should be a string that represents the name of the S3 bucket where the JSON file is located.

    The `json_file_name` parameter should be a string that represents the name of the JSON file to retrieve.

    The method requires AWS credentials with read access to the specified S3 bucket.

    Example usage:
        data = retrieve_json_from_s3("my-bucket", "data.json")
    """
    # Create a session using your AWS credentials
    session = boto3.Session(
        aws_access_key_id="access id",
        aws_secret_access_key="access id",
        region_name="region"
    )
    # Create an S3 client
    s3 = session.client("s3")


    # Retrieve JSON file content from S3
    file_content = s3.get_object(Bucket=bucket_name, Key=json_file_name)['Body'].read().decode('utf-8')

    # Parses JSON content
    data = json.loads(file_content)

    return data


results = retrieve_json_from_s3("blackbaud-testing", "test-constituents.json")

user_id = [ index['id'] for index in results ]


# Replace the following values with your actual API credentials
client_id = 'client_id' #OAuth Client ID
client_secret = 'client_secret' #OAuth Client Secret
redirect_uri = 'https://www.matt-thacker.com/redirect'
scope = 'Full'

# Step 1: Get the authorization URL
authorization_url = 'https://oauth2.sky.blackbaud.com/authorization'
params = {
	'response_type': 'code',
	'client_id': client_id,
	'redirect_uri': redirect_uri,
	'scope': scope
}
url = f'{authorization_url}?{urlencode(params)}'

print(f'Please visit the following URL to authorize the application:\n{url}')

# Step 2: Get the authorization code from the user
auth_code = input('Enter the authorization code you received: ')

# Step 3: Exchange the authorization code for an access token
token_url = 'https://oauth2.sky.blackbaud.com/token'
payload = {
	'grant_type': 'authorization_code',
	'code': auth_code,
	'client_id': client_id,
	'client_secret': client_secret,
	'redirect_uri': redirect_uri
}
response = requests.post(token_url, data=payload)
response_data = response.json()

if 'access_token' not in response_data:
	print(f'Error: {response_data.get("error_description")}')
else:
	access_token = response_data['access_token']
	refresh_token = response_data['refresh_token']
	print(f'Access token: {access_token}\nRefresh token: {refresh_token}')


def get_constituent_code_by_id(constituent_id):
    """
    :param constituent_id: The ID of the constituent whose constituent code is to be retrieved.
    :return: The constituent code of the specified constituent.

    This method retrieves the constituent code of the specified constituent by making a GET request to the Blackbaud SKY API. The constituent ID is provided as a parameter to the method
    *. The method returns the constituent code if the request is successful, otherwise it raises an exception with the error message.

    Example usage:
        constituent_id = 12345
        code = get_constituent_code_by_id(constituent_id)
        print(code)  # Output: "ABC123"
    """
    # Define the endpoint
    endpoint = f"https://api.sky.blackbaud.com/constituent/v1/constituents/{constituent_id}/constituentcodes"

    # Define the headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Bb-Api-Subscription-Key": 'api-key',
        "Content-Type": "application/json"
    }

    # Send the request
    response = requests.get(endpoint, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        ids_with_codes.append(response.json())
        return response.json()
    else:
        # Handle error scenarios
        raise Exception(f"Failed to get constituent by ID: {response.text}")

ids_with_codes = []

for id in user_id:
    try:
        get_constituent_code_by_id(id)
    except Exception as e:
        print(e)

with open('constituent-code-test.json', 'w') as f:
    json.dump(ids_with_codes, f)

df_constituents = pd.DataFrame(results)

results_con_codes = retrieve_json_from_s3("blackbaud-testing", "constituent-code-test.json")

constituent_codes_clean = [ index['value'] for index in results_con_codes ]

df_constitiuent_codes = pd.DataFrame(constituent_codes_clean, columns=['constituent_codes1', 'constituent_codes2'])

col1 = df_constitiuent_codes['constituent_codes1'].to_frame()
col2 = df_constitiuent_codes['constituent_codes2'].to_frame()

col1.dropna(inplace=True)
col2.dropna(inplace=True)

col1.rename(columns={'constituent_codes1': 'constituent_codes'}, inplace=True)
col2.rename(columns={'constituent_codes2': 'constituent_codes'}, inplace=True)

new_df = pd.concat([col1, col2]).reset_index(drop=True)

new_df.explode('constituent_codes')

def explode(vect):
    """
    Explode Method

    Explodes a given vector and extracts specific elements from it.

    :param vect: A dictionary representing the vector with the following keys:
        - 'id': The ID of the vector.
        - 'constituent_id': The constituent ID of the vector.
        - 'description': The description of the vector.
        - 'sequence': The sequence of the vector.

    :return: A tuple containing the exploded elements of the vector in the following order:
        - const_code_id: The ID of the vector.
        - const_id: The constituent ID of the vector.
        - const_code: The description of the vector.
        - const_code_seq: The sequence of the vector.
    """
    const_code_id = vect['id']
    const_id = vect['constituent_id']
    const_code = vect['description']
    const_code_seq = vect['sequence']

    return const_code_id, const_id, const_code, const_code_seq


new_df['const_code_id'], new_df['const_id'], new_df['const_code'], new_df['const_code_seq'] = zip(
    *new_df['constituent_codes'].map(explode))

new_df.drop(columns=['constituent_codes'], inplace=True)

#Merge the two dataframes
df_merged = pd.merge(df_constituents, new_df, left_on='id', right_on='const_id', how='left')