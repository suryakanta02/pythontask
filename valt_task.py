import json
import datetime
import logging
import psycopg2
from psycopg2 import extras


pg_config = {
    "host": "localhost",
    "dbname": "pgsql_target_db",
    "user": "postgres",
    "password": "password",
    "port": 5432
}

schema_name = "public"



logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")



static_tests_data = [
    {
        'id': 'test123',
        'athleteId': 'ath1',
        'hubAthleteId': 'hub1',
        'recordingId': 'rec1',
        'recordedUTC': '2024-07-01T12:00:00Z',
        'recordedTimezone': 'UTC',
        'testType': 'Jump',
        'weight': 70.5,
        'analysedUTC': '2024-07-01T12:30:00Z',
        'analysedTimezone': 'UTC',
        'lastModifiedUTC': '2024-07-01T12:35:00Z'
    }
]

static_trial_data = {
    'test123': [
        {
            'id': 'trial456',
            'athleteId': 'ath1',
            'hubAthleteId': 'hub1',
            'recordedUTC': '2024-07-01T12:01:00.000Z',
            'recordedTimezone': 'UTC',
            'startTime': '00:00:01',
            'endTime': '00:00:05',
            'limb': 'Left',
            'lastModifiedUTC': '2024-07-01T12:36:00.000Z',
            'results': [
                {
                    'resultId': 'res789',
                    'value': 10.5,
                    'time': '00:00:02',
                    'limb': 'Left',
                    'repeat': 1,
                    'definition': {
                        'name': 'Power',
                        'description': 'Explosive power output',
                        'unit': 'Watts',
                        'repeatable': True,
                        'asymmetry': False
                    }
                }
            ]
        }
    ]
}



def insert_rows(sql, rows):
    try:
        conn = psycopg2.connect(**pg_config)
        cur = conn.cursor()
        extras.execute_batch(cur, sql, rows)  # Fast way to insert many rows
        conn.commit()
        logging.info(f"Inserted {len(rows)} rows.")
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting rows: {e}")



def delete_existing_test_data_if_present(test_id, conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {schema_name}.athlete_tests WHERE athlete_test_id = %s", (test_id,))
        exists = cur.fetchone()

        if exists:
            logging.info(f"Test ID {test_id} already exists. Deleting old data.")
            cur.execute(f"DELETE FROM {schema_name}.athlete_test_trial_results WHERE athlete_test_id = %s", (test_id,))
            cur.execute(f"DELETE FROM {schema_name}.athlete_test_trials WHERE athlete_test_id = %s", (test_id,))
            cur.execute(f"DELETE FROM {schema_name}.athlete_tests WHERE athlete_test_id = %s", (test_id,))
            conn.commit()
        else:
            logging.info(f"Test ID {test_id} does not exist. Skipping delete.")


#  Main Sync Function


def sync(date, update_tables=["tests", "trials", "results"]):
    logging.info(f"Starting sync for test data on date: {date}")
    
    try:
        conn = psycopg2.connect(**pg_config)

        test_rows = []
        trial_rows = []
        result_rows = []

        for test in static_tests_data:
            # First, delete if existing
            delete_existing_test_data_if_present(test['id'], conn)

            # Add test row
            test_rows.append((
                test['id'],
                test['athleteId'],
                test['hubAthleteId'],
                test['recordingId'],
                datetime.datetime.fromisoformat(test['recordedUTC'].replace('Z', '+00:00')),
                test['recordedTimezone'],
                test['testType'],
                test['weight'],
                datetime.datetime.fromisoformat(test['analysedUTC'].replace('Z', '+00:00')),
                test['analysedTimezone'],
                json.dumps(test),  
                datetime.datetime.fromisoformat(test['lastModifiedUTC'].replace('Z', '+00:00')),
                datetime.datetime.now(),
                datetime.datetime.now(),
                None,
                None
            ))

            # Add trial rows for this test
            #trials = static_trial_data.get(test['id'], [])
            if test['id'] in static_trial_data:
                trials = static_trial_data[test['id']]
            else:
                trials = []

            for trial in trials:
                trial_rows.append((
                    trial['id'],
                    test['id'],
                    trial['athleteId'],
                    trial['hubAthleteId'],
                    test['recordingId'],
                    datetime.datetime.strptime(trial['recordedUTC'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                    trial['recordedTimezone'],
                    trial['startTime'],
                    trial['endTime'],
                    json.dumps({k: v for k, v in trial.items() if k != "results"}),
                    trial['limb'],
                    datetime.datetime.strptime(trial['lastModifiedUTC'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                    None,
                    None
                ))

                # Add result rows
                for result in trial.get('results', []):
                    result_rows.append((
                        result['resultId'],
                        trial['id'],
                        trial['athleteId'],
                        trial['hubAthleteId'],
                        test['id'],
                        result['value'],
                        result['time'],
                        result['limb'],
                        result['repeat'],
                        result['definition']['name'],
                        result['definition']['description'],
                        result['definition']['unit'],
                        result['definition']['repeatable'],
                        result['definition']['asymmetry'],
                        json.dumps(result),
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                        None,
                        None
                    ))

        # Insert tests
        if "tests" in update_tables:
            insert_rows(f"""
                INSERT INTO {schema_name}.athlete_tests (
                    athlete_test_id, athlete_id, athlete_hub_id, athlete_recording_id,
                    athlete_recorded_utc, athlete_recorded_timezone, athlete_test_type,
                    athlete_weight, athlete_analyzed_utc, athlete_analyzed_timezone,
                    athlete_test_response_data, athlete_test_lastmodified_utc,
                    created_at, updated_at, created_by_user, modified_by_user
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, test_rows)

        # Insert trials
        if "trials" in update_tables:
            insert_rows(f"""
                INSERT INTO {schema_name}.athlete_test_trials (
                    athlete_trial_id, athlete_test_id, athlete_id, athlete_hub_id,
                    athlete_recording_id, athlete_recorded_utc, athlete_recorded_timezone,
                    trial_start_time, trial_end_time, athlete_trial_response_data,
                    athlete_trial_limb, athlete_trial_lastmodified_utc,
                    created_at, updated_at, created_by_user, modified_by_user
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, trial_rows)

        # Insert results
        if "results" in update_tables:
            insert_rows(f"""
                INSERT INTO {schema_name}.athlete_test_trial_results (
                    athlete_trial_result_id, athlete_trial_id, athlete_id, athlete_hub_id,
                    athlete_test_id, result_value, result_time, result_limb, result_repeat,
                    result_name, result_desc, result_unit, result_repeatable, result_assymetry,
                    athlete_result_response_data, created_at, updated_at, created_by_user, modified_by_user
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, result_rows)

        conn.close()

    except Exception as e:
        logging.error(f"Error during sync: {e}")



if __name__ == "__main__":
    sync("2024-07-01")
