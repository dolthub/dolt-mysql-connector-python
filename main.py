import mysql.connector

# configuration to connect to Dolt server
config = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'doltdb',
}

'''
Queries the 'playlist' table in 'doltdb' database.
'''
def select_from_table(cnx: mysql.connector.connection):
  print("\n--querying 'playlist' table\n")
  cursor = cnx.cursor()
  cursor.execute("SELECT id, artist_name, song_name FROM playlist")
  for (id, artist_name, song_name) in cursor:
    print('{}. "{}" by {}'.format(id, song_name, artist_name))
  cursor.close()

'''
Commits any current changes.
'''
def commit(cnx: mysql.connector.connection, msg: str):
  print("\n--committing changes\n")
  cursor = cnx.cursor()
  # equivalent to "CALL DOLT_COMMIT('-Am', 'commit message')"
  cursor.callproc("DOLT_COMMIT", ("-Am", msg))
  cursor.close()

'''
Show diff between current HEAD and the last commit before it.
'''
def diff_on_playlist(cnx: mysql.connector.connection):
  print("\n--showing the diff on 'playlist' table\n")
  cursor = cnx.cursor()
  cursor.execute("select * from dolt_diff('HEAD~', 'HEAD', 'playlist');")
  for (to_id, to_artist_name, to_song_name, to_commit, to_commit_date, 
       from_id, from_artist_name, from_song_name, from_commit, from_commit_date, diff_type) in cursor:
    if diff_type == "added":
      print('row {}: \nTO:   {}, {}, {}\n'.format(diff_type, to_id, to_artist_name, to_song_name))
    elif diff_type == "removed":
      print('row {}: \nFROM: {}, {}, {}\n'.format(diff_type, from_id, from_artist_name, from_song_name))
    else:
      print('row {}: \nFROM: {}, {}, {} \nTO:   {}, {}, {}\n'.format(diff_type, 
       from_id, from_artist_name, from_song_name, to_id, to_artist_name, to_song_name))
  cursor.close()

'''
Insert, delete and update rows on 'playlist' table.
'''
def make_changes_to_table(cnx: mysql.connector.connection):
  print("\n--making changes to 'playlist' table\n")
  cursor = cnx.cursor()
  cursor.execute("insert into playlist (artist_name, song_name) values ('Taylor Swift', 'Paper Rings');")
  cursor.execute("delete from playlist where id = 2;")
  cursor.execute("update playlist set song_name = 'Everything Goes On' where artist_name = 'Porter Robinson';")
  cursor.close()

try:
  cnx = mysql.connector.connect(**config)
  print("Successfully connected to Dolt server!")
  select_from_table(cnx)
  make_changes_to_table(cnx)
  commit(cnx, "commit insert, delete and update changes")
  diff_on_playlist(cnx)
  select_from_table(cnx)
except mysql.connector.Error as err:
    print(err)
else:
  print("Disconnecting from Dolt server!")
  cnx.close()
