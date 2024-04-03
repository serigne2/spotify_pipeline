import mysql.connector
from sqlalchemy import create_engine
from transform import transform
import pandas as pd
def load():
    song_df = transform()[0]
    album_df=transform()[1]
    artist_df=transform()[2]
    try:
        # Établir une connexion à la base de données MySQL
        conn = mysql.connector.connect(
           host="localhost",
           user="",
           password="",
           database="spotify_schema"
           )
        cur = conn.cursor()


        engine = create_engine("mysql+mysqlconnector://root:palaye12@localhost/spotify_schema")
        conn_eng = engine.raw_connection()
        cur_eng = conn_eng.cursor()

        # TRACKS: Temp Table
        # Création de la table temporaire pour les pistes dans MySQL
        cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_track
        SELECT * FROM spotify_track LIMIT 0
        """)
        # Utilisation de la méthode to_sql pour insérer les données dans la table temporaire
        song_df.to_sql("tmp_track", con=engine, if_exists='append', index=False)

        # Moving data from temp table to production table
        # Déplacer les données de la table temporaire vers la table de production
        cur.execute(
            """
            INSERT INTO spotify_track
            SELECT tmp_track.*
            FROM   tmp_track
            LEFT   JOIN spotify_track USING (unique_identifier)
            WHERE  spotify_track.unique_identifier IS NULL
            """
        )

        # Supprimer la table temporaire
        cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_track")

        # Valider la transaction
        conn.commit()

        # ALBUM: Temp Table
        # Création de la table temporaire pour les albums dans MySQL
        cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_album
        SELECT * FROM spotify_album LIMIT 0
        """)

        # Utilisation de la méthode to_sql pour insérer les données dans la table temporaire
        album_df.to_sql("tmp_album", con=engine, if_exists='append', index=False)

        # Valider la transaction
        conn.commit()

        # Moving from Temp Table to Production Table
        # Déplacer les données de la table temporaire vers la table de production
        cur.execute(
            """
            INSERT INTO spotify_album
            SELECT tmp_album.*
            FROM   tmp_album
            LEFT   JOIN spotify_album USING (album_id)
            WHERE  spotify_album.album_id IS NULL
            """
        )

        # Supprimer la table temporaire
        cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_album")

        # Valider la transaction
        conn.commit()

        cur.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_artist
        SELECT * FROM spotify_artists LIMIT 0
        """)

        # Utilisation de la méthode to_sql pour insérer les données dans la table temporaire
        artist_df.to_sql("tmp_artist", con=engine, if_exists='append', index=False)

        # Valider la transaction
        conn.commit()

        # Moving data from temp table to production table
        # Déplacer les données de la table temporaire vers la table de production
        cur.execute(
            """
            INSERT INTO spotify_artists
            SELECT tmp_artist.*
            FROM   tmp_artist
            LEFT   JOIN spotify_artists USING (artist_id)
            WHERE  spotify_artists.artist_id IS NULL
            """
        )

        # Supprimer la table temporaire
        cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_artist")

        # Valider la transaction
        conn.commit()

        # Fermer la connexion à la base de données
        cur.close()
        conn.close()

        return "Finished Extract, Transform, Load - Spotify"

    except Exception as e:
        #Capturer et afficher l'exception en cas d'erreur
        print("An error occurred:", str(e))
        return "Failed to execute ETL process"
print(load())