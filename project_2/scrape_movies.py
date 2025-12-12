import psycopg2
import requests
import time

# ======================== Configuration ========================
# TMDB API key for accessing The Movie Database API
TMDB_API_KEY = "164b934b951df62873cfdd4aaf122f41"

# Database connection configuration
DB_CONFIG = {
    'dbname': 'films',
    'user': 'manfred',
    'password': '112358',
    'host': 'localhost',
    'port': '5432'
}

# Scraping parameters
START_YEAR = 2018
END_YEAR = 2025
TARGET_COUNT = 200  # Target number of movies per year
MAX_PAGES = 15  # Maximum pages to fetch from TMDB API
DEFAULT_COUNTRY = 'us'  # Default country code if not found
# ==============================================================

def get_db_connection():
    """Establish and return a PostgreSQL database connection.
    
    Returns:
        psycopg2.connection: Database connection object.
    """
    return psycopg2.connect(**DB_CONFIG)

def fetch_json(url, params):
    """Fetch JSON data from a URL with retry logic.
    
    Args:
        url (str): The URL to fetch from.
        params (dict): Query parameters.
        
    Returns:
        dict: JSON response or None if request fails after retries.
    """
    params['api_key'] = TMDB_API_KEY
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(2)  # Rate limit: wait before retry
        except:
            pass
        time.sleep(0.5)
    return None

def get_person_dates(person_tmdb_id):
    """Fetch birth year and death year for a person from TMDB.
    
    Args:
        person_tmdb_id (int): The TMDB ID of the person.
        
    Returns:
        tuple: (birth_year, death_year) where death_year is None if still alive.
    """
    data = fetch_json(f"https://api.themoviedb.org/3/person/{person_tmdb_id}", {})
    born = 0
    died = None
    if data:
        if data.get('birthday'):
            try: born = int(data['birthday'][:4])
            except: born = 0
        if data.get('deathday'):
            try: died = int(data['deathday'][:4])
            except: died = None
    return born, died

def get_movie_details(movie_tmdb_id):
    """Fetch detailed movie information from TMDB API.
    
    Args:
        movie_tmdb_id (int): The TMDB ID of the movie.
        
    Returns:
        dict: Movie details or None if request fails.
    """
    return fetch_json(f"https://api.themoviedb.org/3/movie/{movie_tmdb_id}", {})

def get_max_id(cur, table, id_column):
    """Get the maximum ID value from a table.
    
    Args:
        cur (psycopg2.cursor): Database cursor.
        table (str): Table name.
        id_column (str): ID column name.
        
    Returns:
        int: Maximum ID value or 0 if table is empty.
    """
    cur.execute(f"SELECT MAX({id_column}) FROM {table}")
    res = cur.fetchone()[0]
    return res if res is not None else 0

def process_data():
    conn = get_db_connection()
    cur = conn.cursor()

    # Cache valid country codes from database
    cur.execute("SELECT country_code FROM countries")
    valid_countries = set(row[0].strip().lower() for row in cur.fetchall())

    # Initialize current max IDs for movies and people
    current_max_movie_id = get_max_id(cur, 'movies', 'movieid')
    current_max_people_id = get_max_id(cur, 'people', 'peopleid')
    
    print(f"起始 MovieID: {current_max_movie_id}, PeopleID: {current_max_people_id}")

    try:
        for year in range(START_YEAR, END_YEAR + 1):
            # Check if target count for this year is already reached
            cur.execute("SELECT COUNT(*) FROM movies WHERE year_released = %s", (year,))
            count = cur.fetchone()[0]
            if count >= TARGET_COUNT:
                print(f"--- {year} target reached ({count}), skipping ---")
                continue

            print(f"--- Processing {year} (current: {count}) ---")

            for page in range(1, MAX_PAGES + 1):
                cur.execute("SELECT COUNT(*) FROM movies WHERE year_released = %s", (year,))
                if cur.fetchone()[0] >= TARGET_COUNT: break

                data = fetch_json("https://api.themoviedb.org/3/discover/movie", {
                    'primary_release_year': year,
                    'sort_by': 'popularity.desc',
                    'include_adult': 'false',
                    'page': page
                })
                
                if not data or 'results' not in data: continue

                for m in data['results']:
                    cur.execute("SELECT COUNT(*) FROM movies WHERE year_released = %s", (year,))
                    if cur.fetchone()[0] >= TARGET_COUNT: break

                    tmdb_id = m['id']
                    details = get_movie_details(tmdb_id)
                    if not details: continue

                    title = details['title'][:100]
                    origin = details.get('origin_country', [])
                    country_code = DEFAULT_COUNTRY
                    if origin and origin[0].lower() in valid_countries:
                        country_code = origin[0].lower()

                    # Check for duplicate movies (by title, country, and year)
                    cur.execute("SELECT 1 FROM movies WHERE title=%s AND country=%s AND year_released=%s", (title, country_code, year))
                    if cur.fetchone():
                        continue

                    # Insert movie into database
                    current_max_movie_id += 1
                    movie_db_id = current_max_movie_id
                    runtime = details.get('runtime', 0) or 0
                    
                    try:
                        cur.execute("INSERT INTO movies (movieid, title, country, year_released, runtime) VALUES (%s, %s, %s, %s, %s)",
                                    (movie_db_id, title, country_code, year, runtime))
                    except Exception as e:
                        conn.rollback()
                        current_max_movie_id -= 1
                        continue

                    # Process credits: fetch directors and cast members
                    credits = fetch_json(f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits", {})
                    if credits:
                        people_list = []
                        # Add directors (up to 5)
                        for x in credits.get('crew', [])[:5]:
                            if x['job'] == 'Director':
                                people_list.append((x, 'D'))
                        # Add cast members (up to 5)
                        for x in credits.get('cast', [])[:5]:
                            people_list.append((x, 'A'))

                        for p_data, role in people_list:
                            full = p_data['name'].strip()
                            if ' ' in full: parts = full.rsplit(' ', 1); fname, sname = parts[0][:30], parts[1][:30]
                            else: fname, sname = "", full[:30]
                            
                            # Map TMDB gender to our format: 1=Female, 2=Male, 0/other=Unknown
                            g_val = p_data.get('gender', 0)
                            gender = 'F' if g_val == 1 else ('M' if g_val == 2 else '?')

                            # Check if person already exists in database
                            cur.execute("SELECT peopleid FROM people WHERE surname=%s AND first_name=%s", (sname, fname))
                            res = cur.fetchone()
                            
                            if res:
                                pid = res[0]
                            else:
                                # Person not found, create new entry
                                current_max_people_id += 1
                                pid = current_max_people_id
                                # Fetch birth and death dates from TMDB
                                born, died = get_person_dates(p_data['id'])
                                try:
                                    cur.execute("INSERT INTO people (peopleid, first_name, surname, born, died, gender) VALUES (%s, %s, %s, %s, %s, %s)",
                                                (pid, fname, sname, born, died, gender))
                                except:
                                    conn.rollback()
                                    current_max_people_id -= 1
                                    continue
                            
                            try:
                                cur.execute("INSERT INTO credits (movieid, peopleid, credited_as) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                                            (movie_db_id, pid, role))
                            except: conn.rollback()
                    
                    print(f"  + {title}")
                    conn.commit()

            conn.commit()

    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    process_data()