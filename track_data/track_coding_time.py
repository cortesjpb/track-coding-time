import hashlib
import json
import os
import sys

from dotenv import dotenv_values
from rauth import OAuth2Service
from sqlalchemy import create_engine


if sys.version_info[0] == 3:
    raw_input = input


def get_and_upload_data(engine, session):
    print('Getting current user from API...')
    user = session.get('users/current').json()
    print('Authenticated via OAuth as {0}'.format(user['data']['email']))
    print("Getting user's coding stats from API...")
    projects_response = session.get('users/current/projects')
    projects_data = json.loads(projects_response.text)["data"]
    project_names = [project['name'] for project in projects_data]

    with engine.connect() as connection:
        print('Creating table if not exists...')
        create_table_query = """
                                  CREATE TABLE IF NOT EXISTS coding_time_track
                                  (
                                      id serial PRIMARY KEY,
                                      coding_date DATE NOT NULL,
                                      editor VARCHAR(100) NOT NULL,
                                      proyecto VARCHAR(100) NOT NULL,
                                      archivo VARCHAR(250) NOT NULL,
                                      extension VARCHAR(50) NOT NULL,
                                      tiempo_total DECIMAL(10, 2) NOT NULL
                                  );
                             """
        connection.execute(create_table_query)
        print('Getting project names...')
        query = f"""
                    INSERT INTO coding_time_track(fecha,editor,proyecto,archivo,extension,tiempo_total) VALUES
                 """
        rows = []
        for project in project_names:
            project_response = session.get(f'users/current/summaries?project={project}&range=last+7+days+from+yesterday')
            project_details = json.loads(project_response.text)["data"]
            for day in project_details:
                date = day['range']['date']
                if len(day['editors']):
                    editor = day['editors'][0]['name']
                    for entity in day['entities']:
                        file_name = entity['name']
                        file_extension = file_name.split('.')[-1]
                        total_seconds = str(entity['total_seconds'])
                        rows.append(f"""
                                    (CAST('{date}' AS DATE), '{editor}', '{project}', '{file_name}', '{file_extension}', {total_seconds})
                                     """)
        final_query = query + ','.join(rows)
        connection.execute(final_query)
        print('Data uploaded successfully!')

def get_config():
    config = dotenv_values(".env")
    return config


def get_postgres_connector(string_connection):
    engine = create_engine(string_connection)
    return engine


def get_session(service):
    redirect_uri = 'https://wakatime.com/oauth/test'
    state = hashlib.sha1(os.urandom(40)).hexdigest()
    params = {'scope': 'email,read_stats,read_logged_time,write_logged_time',
              'response_type': 'code',
              'state': state,
              'redirect_uri': redirect_uri}
    url = service.get_authorize_url(**params)

    print('**** Visit this url in your browser ****'.format(url=url))
    print('*' * 80)
    print(url)
    print('*' * 80)
    print('**** After clicking Authorize, paste code here and press Enter ****')
    code = raw_input('Enter code from url: ')

    # Make sure returned state has not changed for security reasons, and exchange
    # code for an Access Token.
    headers = {'Accept': 'application/x-www-form-urlencoded'}
    print('Getting an access token...')
    session = service.get_auth_session(headers=headers,
                                       data={'code': code,
                                             'grant_type': 'authorization_code',
                                             'redirect_uri': redirect_uri})
    return session


def get_wt_service(client_id, client_secret):
    service = OAuth2Service(
        client_id=client_id,
        client_secret=client_secret,
        name='wakatime',
        authorize_url='https://wakatime.com/oauth/authorize',
        access_token_url='https://wakatime.com/oauth/token',
        base_url='https://wakatime.com/api/v1/')
    return service


if __name__ == '__main__':
    env_config = get_config()
    pg_string_connection = env_config['DB_URI']
    client_id = env_config['WT_CLIENT_ID']
    client_secret = env_config['WT_CLIENT_SECRET']
    wt_service = get_wt_service(client_id, client_secret)
    wt_session = get_session(wt_service)
    pg_engine = get_postgres_connector(pg_string_connection)
    get_and_upload_data(pg_engine, wt_session)
