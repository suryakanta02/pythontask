# import psycopg2


# conn = psycopg2.connect(
#     host="localhost",
#     database="testdb",
#     user="postgres",         
#     password="password", 
#     port=5432
# )

# conn.autocommit = True
# cur = conn.cursor()

# fetch_query = """
# SELECT schemaname, relname
# FROM pg_stat_user_tables
# WHERE n_dead_tup > 1000
# ORDER BY last_autovacuum DESC;
# """

# cur.execute(fetch_query)
# tables = cur.fetchall()

# print("Found", len(tables), "tables with dead tuples > 1000.")

# for schemaname, relname in tables:
#     full_table_name = f"{schemaname}.{relname}"
#     vacuum_query = f"VACUUM ANALYZE {full_table_name};"
    
#     print("Running:", vacuum_query)
    
#     try:
#         cur.execute(vacuum_query)
#         print("Vacuumed", full_table_name)
#     except Exception as e:
#         print("Failed to vacuum", full_table_name, ":", e)

# cur.close()
# conn.close()
# print("Done.")

# new way to write with out function 

# import os
# import psycopg2
# import logging
# from dotenv import load_dotenv


# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
    
# )


# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "database": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
#     "port": os.getenv("DB_PORT")
# }
# logging.info("new work")

# try:
#     conn = psycopg2.connect(**DB_CONFIG)
#     conn.autocommit = True
#     cur = conn.cursor()

#     fetch_query = """
#     SELECT schemaname, relname
#     FROM pg_stat_user_tables
#     WHERE n_dead_tup > 1000
#     ORDER BY last_autovacuum DESC;
#     """

#     cur.execute(fetch_query)
#     tables = cur.fetchall()

#     logging.info("Found %d tables with dead tuples > 1000.", len(tables))

#     for schemaname, relname in tables:
#         full_table_name = f"{schemaname}.{relname}"
#         vacuum_query = f"VACUUM ANALYZE {full_table_name};"

#         logging.info("Running: %s", vacuum_query)

#         try:
#             cur.execute(vacuum_query)
#             logging.info("Vacuumed %s", full_table_name)
#         except Exception as e:
#             logging.error("Failed to vacuum %s: %s", full_table_name, e)

#     cur.close()
#     conn.close()
#     logging.info("Done.")

# except Exception as conn_error:
#     logging.critical("Database connection failed: %s", conn_error)


# CREATE TABLE weather_data (
#     id SERIAL PRIMARY KEY,
#     city VARCHAR(50),
#     country VARCHAR(50),
#     temperature_c FLOAT,
#     condition TEXT,
#     wind_kph FLOAT,
#     humidity INT,
#     air_quality JSONB,
#     observation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );

import os, requests, logging, psycopg2, json, datetime, time, sys
from psycopg2 import extras
from dotenv import load_dotenv

team_id_hardcoded = 'f926f34e-5d84-4b2d-91af-5f9fe2cbd786' #TODO: check if needs to be .env or sql

load_dotenv()

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',filename='out.log', level=int(os.getenv('LOGGING')))

if len(sys.argv) != 2:
    raise AssertionError("Please provide a runtype: 'full' 'signup' 'delta'\n if you want to run full python vald.py full")
else:
    runtype = sys.argv[1]

logging.info(f"runtype = {runtype}")
api_call = 0

postgres_conn_params = {
    'host': os.getenv('PG_HOST'),
    'database': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'port': os.getenv('PG_PORT')
}
schema_name = os.getenv('pg_schema_name')

token_url = os.getenv('token_url')
test_url = os.getenv('test_url')
profile_url = os.getenv('profile_url')
client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
sql_bulk_batch = int(os.getenv('insert_batch_size',1000))
last_token_time = None
date_from = os.getenv('date_from')      #'2024-04-10T10:14:17.609454Z'
logging.info(f"date_from(default): {date_from}")

signup_athlete = os.getenv('signup_athlete')
signup_delay = int(os.getenv('signup_delay'))

def call_sproc(pg_config, sproc_addr, cur_prefix="cursor", cursors=1, optional_parameters=[]):
    rows=[]
    try:
        conn = psycopg2.connect(**pg_config)
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        logging.debug("Calling Sproc."+str(sproc_addr)+str(optional_parameters))
        cur.callproc( sproc_addr, optional_parameters + [f'{cur_prefix}{i}' for i in range(cursors)])
        logging.debug("Fetching Cursors.")
        for i in range(cursors):
            cur.execute(f'FETCH ALL IN "{cur_prefix}{i}"')
            rows.append(cur.fetchall())
            logging.info(f"{len(rows[i])} fetched from PSQL")
        conn.commit()
    except Exception as e:
        logging.error(f"Error during calling sproc:{e}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return rows

def get_token(client_id, client_secret, token_url):
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }
    response = requests.post(token_url, data=data)
    if response.status_code == 200:
        global last_token_time
        last_token_time = time.time()
        return response.json().get('access_token')
    else:
        raise Exception(f"Failed to obtain token: {response.status_code} {response.text}")

try:
    token = get_token(client_id, client_secret, token_url)
except Exception as e:
    logging.error("fetching token failed at program begining")
    raise e

def fetch_api_data(access_token, method, url, params=None, pages = False, accept = 'application/json'):
    try:
        logging.debug(f"last token time: {last_token_time}; current time {time.time()}")
        if time.time()-last_token_time>7000:
            global token
            token = get_token(client_id, client_secret, token_url)
        headers = {
            'Authorization': f'Bearer {access_token}',
            'accept': accept,
        }
        response = requests.request(method, url, headers=headers, params=params)
        logging.debug(f"{response.json()}")
        global api_call
        api_call+=1
        if pages and response.status_code == 200:
            data = response.json().get('items')
            while response.json().get('currentPage')<response.json().get('totalPages'):
                url = '/'.join(url.split('/')[:-1]+[f'{response.json().get("currentPage")+1}'])
                response = requests.request(method, url, headers=headers, params=params)
                api_call+=1
                logging.debug(f"{data}")
                data.extend(response.json().get('items'))
            logging.debug(f"{data}")
            return data
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"API request failed: {url}; {params} {response.status_code} {response.text}")
    except Exception as e:
        logging.error(f"{e}: API request failed: {url}; {params} {response.status_code} '{response.text}'")


def insert_rows(sql_statement, rows, bulk=True):
    connection = psycopg2.connect(**postgres_conn_params)
    cursor = connection.cursor()
    if bulk:
        i=0
        except_enter = False
        for i in range(0, len(rows), sql_bulk_batch):
            try:
                if cursor.closed:
                    connection = psycopg2.connect(**postgres_conn_params)
                    cursor = connection.cursor()
                cursor.executemany(sql_statement, rows[i:i+sql_bulk_batch])
                logging.info(f"Insert successful upto: {min(i+sql_bulk_batch, len(rows))}")
                connection.commit()
            except Exception as e:
                logging.error(f"PGSQL: {e} \nError in a row after {i}")
                connection.rollback()
                new_rows = rows[i:]
                rows = new_rows
                except_enter = True
                break
        if not except_enter:
            logging.info(f"SQL insert successful")
            return True
    for row in rows:
        try:
            if cursor.closed:
                connection = psycopg2.connect(**postgres_conn_params)
                cursor = connection.cursor()
            cursor.execute(sql_statement, row)
            connection.commit()
        except Exception as e:
            logging.error(f"PGSQL: {e}\n{row}")
            connection.rollback()
    cursor.close()
    return True

def sync(date, update_tables = ["teams", "tests", "trials", "results", "profiles"]):
    logging.info(f"Syncing data in {update_tables} from {date}")
    try:
        teams_url = f'{test_url}/v2019q3/teams'
        teams_data = fetch_api_data(token, 'GET', teams_url)
        logging.info(f"Number of teams:{len(teams_data)}")
        sql = f"""INSERT INTO {schema_name}.teams (
                team_id,
                team_name,
                team_region,
                team_hub_sync_enabled,
                team_api_response_data,
                created_at,
                updated_at,
                created_by_user,
                modified_by_user
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            );"""
        if "teams" in update_tables:
            insert_rows(
                sql,
                [(item.get('id'), item.get('name'), item.get('region'), item.get('isHubSyncEnabled'), json.dumps(item), datetime.datetime.now(), datetime.datetime.now(),None,None)  for item in teams_data]
            )
        trial_rows = []
        result_rows = []
        if not teams_data:
            raise ValueError("Teams data Empty")
        for team in teams_data:
            logging.info(f"inserting in tables {update_tables}")
            if 'tests' not in update_tables \
                    and 'trials' not in update_tables\
                    and 'results' not in update_tables:
                pass
            else:
                tests_url = f'{test_url}/v2019q3/teams/{team["id"]}/tests/1'
                tests_data = fetch_api_data(token, 'GET', tests_url, {'modifiedFrom': date}, pages= True)
                test_rows = []
                if not tests_data:
                    logging.error(f"{tests_url} fetched None")
                    continue
                logging.info(f"Number of tests:{len(tests_data)}")
                for test in tests_data:
                    logging.debug(f"TestID: {test.get('id')}")
                    test_rows.append(
                        (test.get('id'), test.get('athleteId'), test.get('hubAthleteId'), test.get('recordingId'), datetime.datetime.fromisoformat(test.get('recordedUTC').replace('Z', '+00:00')), test.get('recordedTimezone'), test.get('testType'), test.get('weight'), datetime.datetime.fromisoformat(test.get('analysedUTC').replace('Z', '+00:00')), test.get('analysedTimezone'), json.dumps(test), datetime.datetime.fromisoformat(test.get('lastModifiedUTC').replace('Z', '+00:00')), datetime.datetime.now(), datetime.datetime.now(), None, None)
                                     )
                    trial_url = f'{test_url}/v2019q3/teams/{team["id"]}/tests/{test["id"]}/trials'
                    trial_data = fetch_api_data(token, 'GET', trial_url)
                    if not trial_data:
                        logging.error(f"{trial_url} fetched None")
                        continue
                    logging.info(f"trial_data count for test id {test.get('id')} {len(trial_data)}")
                    for trial_row in trial_data:
                        logging.debug(f"TrialId: {trial_row.get('id')}")
                        trial_rows.extend([(
                            trial_row.get('id'),
                            test.get('id'),
                            trial_row.get('athleteId'),
                            trial_row.get('hubAthleteId'),
                            test.get('recordingId'),
                            datetime.datetime.strptime(trial_row.get('recordedUTC'), '%Y-%m-%dT%H:%M:%S.%fZ'),
                            trial_row.get('recordedTimezone'),
                            trial_row.get('startTime'),
                            trial_row.get('endTime'),
                            json.dumps({k: v for k, v in trial_row.items() if k != "results"}),
                            trial_row.get('limb'),
                            datetime.datetime.strptime(trial_row.get('lastModifiedUTC'), '%Y-%m-%dT%H:%M:%S.%fZ'),
                            datetime.datetime.now(),
                            datetime.datetime.now(),
                            None,
                            None
                        )]
                    )
                        result_rows.extend([(
                            result.get('resultId'),
                            trial_row.get('id'),
                            trial_row.get('athleteId'),
                            trial_row.get('hubAthleteId'),
                            test.get('id'),
                            result.get('value'),
                            result.get('time'),
                            result.get('limb'),
                            result.get('repeat'),
                            result.get('definition').get('name'),
                            result.get('definition').get('description'),
                            result.get('definition').get('unit'),
                            result.get('definition').get('repeatable'),
                            result.get('definition').get('asymmetry'),
                            json.dumps(result),
                            datetime.datetime.now(),
                            datetime.datetime.now(),
                            None,
                            None
                        )for result in trial_row.get('results')]
                    )
                        logging.info(f" number of result_rows for trial id {trial_row.get('id')} :- {len(result_rows)}")

            sql = f"""INSERT INTO {schema_name}.athlete_tests (
                athlete_test_id,
                athlete_id,
                athlete_hub_id,
                athlete_recording_id,
                athlete_recorded_utc,
                athlete_recorded_timezone,
                athlete_test_type,
                athlete_weight,
                athlete_analyzed_utc,
                athlete_analyzed_timezone,
                athlete_test_response_data,
                athlete_test_lastmodified_utc,
                created_at,
                updated_at,
                created_by_user,
                modified_by_user
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );"""
            if "tests" in update_tables:
                insert_rows(sql, test_rows)
            sql = f"""
                INSERT INTO {schema_name}.athlete_test_trials (
                    athlete_trial_id,
                    athlete_test_id,
                    athlete_id,
                    athlete_hub_id,
                    athlete_recording_id,
                    athlete_recorded_utc,
                    athlete_recorded_timezone,
                    trial_start_time,
                    trial_end_time,
                    athlete_trial_response_data,
                    athlete_trial_limb,
                    athlete_trial_lastmodified_utc,
                    created_at,
                    updated_at,
                    created_by_user,
                    modified_by_user
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """
            if "trials" in update_tables:
                logging.info(f"Number of trials total: {len(trial_rows)} ")
                insert_rows(sql, trial_rows )
            sql = f"""
                INSERT INTO {schema_name}.athlete_test_trial_results (
                    athlete_trial_result_id,
                    athlete_trial_id,
                    athlete_id,
                    athlete_hub_id,
                    athlete_test_id,
                    result_value,
                    result_time,
                    result_limb,
                    result_repeat,
                    result_name,
                    result_desc,
                    result_unit,
                    result_repeatable,
                    result_assymetry,
                    athlete_result_response_data,
                    created_at,
                    updated_at,
                    created_by_user,
                    modified_by_user
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """
            if "results" in update_tables:
                logging.info(f"Number of results total: {len(result_rows)} ")
                insert_rows(sql, result_rows)
            if not "profiles" in update_tables:
                continue


            ###Athletes
            athletes_url = f'{test_url}/v2019q3/teams/{team["id"]}/athletes'
            athletes_data = fetch_api_data(token, 'GET', athletes_url, {'modifiedFrom': '2024-01-26T02:42:24.575Z'})
            athlete_rows=[]
            for athlete in athletes_data:
                athlete_profile_url = f'{profile_url}/profiles/{athlete["id"]}'
                athlete_profile = fetch_api_data(token, 'GET', athlete_profile_url, {'TenantId': team.get('id')}, accept = 'text/plain')
                athlete_rows.extend(
                    [(
                        athlete.get('id'),
                        athlete.get('teamId'),
                        athlete.get('hubId'),
                        athlete_profile.get('syncId'),
                        athlete.get('id'),
                        athlete.get('externalId'),
                        athlete.get('name'),
                        athlete.get('givenName'),
                        athlete.get('familyName'),
                        athlete_profile.get('dateOfBirth'),
                        athlete_profile.get('sex'),
                        athlete_profile.get('email'),
                        athlete_profile.get('weightInKg'),
                        athlete_profile.get('heightInCm'),
                        json.dumps(athlete_profile),
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                        None,
                        None
                )]
                )
            sql = f"""
                    INSERT INTO {schema_name}.athlete_profiles (
                        athlete_id,
                        team_id,
                        athlete_hub_id,
                        athlete_sync_id,
                        athlete_profile_id,
                        athlete_external_id,
                        athlete_name,
                        athlete_given_name,
                        athlete_family_name,
                        athlete_dob,
                        athlete_gender,
                        athlete_email,
                        athlete_weight_kgs,
                        athlete_height_cms,
                        athlete_profile_response_data,
                        created_at,
                        updated_at,
                        created_by_user,
                        modified_by_user
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    );
                """

            logging.info(f"Number of athletes total: {len(athlete_rows)} ")
            insert_rows(sql, athlete_rows)
    except Exception as e:
        logging.error(f"Program ended unsuccessfully: {e}")

def create_profiles(import_url):
    profiles = call_sproc(postgres_conn_params, f"{schema_name}.get_athletes_to_create_vald_profiles")[0]
    logging.info(f"Number of profiles:{len(profiles)}")
    logging.debug(f"Profiles\n{profiles}")
    headers = {
    'Authorization': f'Bearer {token}',
    'accept': '*/*',
    'Content-Type': 'application/json-patch+json'
    }
    signup_success_athletes = []
    for athlete in profiles:
        try:
            logging.info(f"athlete: {athlete}")
            data = \
            {
                'dateOfBirth': athlete.get('birthdate').strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                'email': athlete.get('email', ''),
                'givenName': athlete.get('firstname', ''),
                'familyName': athlete.get('lastname', ''),
                'tenantId': athlete.get('teamid', team_id_hardcoded),
                'sex': athlete.get('gender', 'Unknown'),                        # only takes values Male, Female, Unknown.
                'syncId': athlete.get('daysmart_id', ''),                       # using daysmart id for sync id
                'externalId': athlete.get('daysmart_id', '')
            }

            if signup_athlete in [True, 1, 'True', 'true']:
                response = requests.post(import_url, json=data, headers=headers)
                if response.status_code == 204:
                    logging.info(f"{data} signed UP successfully!")
                    signup_success_athletes.append(athlete)

                else:
                    logging.error(f"API call failed with status code {response.status_code}\n{athlete}")
            else:
                logging.warning(f"athlete signup call set to {signup_athlete}: so no athletes were signed up\n{data} ")
                with open('signup_data.log', 'a') as signup_data:
                    signup_data.write(datetime.datetime.now().isoformat())
                    signup_data.write(f"on {datetime.datetime.now()} :{data}\n")
        except Exception as e:
            logging.error(f"failed athlete signup {athlete}:- {e}")

    time.sleep(signup_delay)

    for athlete in signup_success_athletes:
        profiles_url = f'{profile_url}/profiles'
        vald_profile = fetch_api_data(token, 'GET', profiles_url, {'TenantId' : team_id_hardcoded, 'SyncId' : athlete.get('daysmart_id')})
        if not vald_profile:
            logging.error(f"Fetched none for athlete {athlete} after sign up.")
            print(f"{athlete} not inserted after signup.")
            break
        athlete_profile_url = f'{profile_url}/profiles/{vald_profile.get("profiles")[0].get("profileId")}'
        athlete_profile = fetch_api_data(token, 'GET', athlete_profile_url, {'TenantId': team_id_hardcoded}, accept = 'text/plain')
        logging.info(f"Inserting into athlete_profile table {athlete_profile}")

        sql = f"""
            INSERT INTO {schema_name}.athlete_profiles (
                athlete_id,
                team_id,
                athlete_hub_id,
                athlete_sync_id,
                athlete_profile_id,
                athlete_external_id,
                athlete_name,
                athlete_given_name,
                athlete_family_name,
                athlete_dob,
                athlete_gender,
                athlete_email,
                athlete_weight_kgs,
                athlete_height_cms,
                athlete_profile_response_data,
                created_at,
                updated_at,
                created_by_user,
                modified_by_user
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """
        insert_rows(
            sql,
            [(
                athlete_profile.get('profileId'),
                team_id_hardcoded,
                athlete_profile.get('profileId'),
                athlete_profile.get('syncId'),
                athlete_profile.get('profileId'),
                athlete_profile.get('externalId'),
                f"{athlete_profile.get('givenName')} {athlete_profile.get('familyName')}",
                athlete_profile.get('givenName'),
                athlete_profile.get('familyName'),
                athlete_profile.get('dateOfBirth'),
                athlete_profile.get('sex'),
                athlete_profile.get('email') if athlete_profile.get('email') else athlete.get('email'),
                athlete_profile.get('weightInKg'),
                athlete_profile.get('heightInCm'),
                json.dumps(athlete_profile),
                datetime.datetime.now(),
                datetime.datetime.now(),
                None,
                None
            )]
        )
        if athlete.get('copaid'):
            try:
                logging.info(f"updating vald id for{athlete}")
                call_sproc(
                    postgres_conn_params,
                    f"{schema_name}.update_athlete_vald_id",
                    cursors = 0,
                    optional_parameters = [
                        athlete.get('copaid'),
                        athlete.get('daysmart_id'),
                        athlete_profile.get('profileId')
                        ]
                    )
            except Exception as e:
                logging.error(f"Unable to update {athlete} vald_id:\n{e}")


def test_results():
    conn = psycopg2.connect(**postgres_conn_params)
    cur = conn.cursor()
    cur.callproc(f"{schema_name}.get_max_vald_results_date")
    result = cur.fetchone()[0]
    if result:
        latest_ts = result.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging.info(f"date_from(psql ls_timestamp): {latest_ts}")
        sync(
            date = latest_ts,
            update_tables =[
                "tests",
                "trials",
                "results",
            ]
        )
    else:
        logging.error(f"No previous data to fetch latest date.")

def delta_sync():
    if runtype == "signup":
        create_profiles(f'{profile_url}/profiles/import')
    elif runtype == "delta":
        test_results()
    else:
        raise LookupError(f"Program has no runtype {runtype}")


if __name__ == "__main__":
    try:
        if runtype == "full":
            sync(date_from)
        else:
            delta_sync()
        logging.info(f"api calls:{api_call}")
    except Exception as e:
        logging.error("program unsuccessful: {e}")