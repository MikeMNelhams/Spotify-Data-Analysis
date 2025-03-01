import urllib.parse
from urllib import request
import urllib.response
from urllib.error import HTTPError

import pandas as pd
from file_operations_mn.file_writers import TextWriterSingleLine
from api_exceptions import API_BatchSizeTooLargeError, API_RateLimitError

import time
import datetime
import json
import pyodbc

from itertools import batched


REDIRECT_URI = "data-analysis-mn-login://callback"

SERVER = TextWriterSingleLine("SQL_Server_name.txt").load()
DATABASE = "spotify_data"
API_MAX_TRACK_IDS_BATCH_SIZE = 50

ACCESS_TOKEN_PATH = "access_token.txt"


class Artist:
    def __init__(self, name: str, artist_id: str):
        self.name = name
        self.artist_id = artist_id

    def __repr__(self) -> str:
        return f"Artist[{self.name}, id={self.artist_id}]"


class TrackMinimal:
    def __init__(self, name: str, artists: list[Artist], track_id: str, album_name: str, album_id: str, duration_ms: int, is_local: bool):
        self.name = name
        self.artists = artists
        self.main_artist = artists[0].name

        self.track_id = track_id
        self.album_name = album_name
        self.album_id = album_id
        self.duration_ms = duration_ms
        self.is_local = is_local

    def __repr__(self) -> str:
        return f"Album[{self.name}, {self.artists}, id={self.album_id}]"

    def to_list(self) -> list[str | int | bool]:
        return [self.name, self.main_artist, self.track_id, self.album_name, self.album_id, self.duration_ms, self.is_local]

    @property
    def columns(self) -> list[str]:
        return ["name", "main_artist", "track_id", "album_name", "album_id", "duration_ms", "is_local"]


class Tracks:
    def __init__(self, tracks: list[TrackMinimal]):
        self.tracks = tracks

    def __repr__(self) -> str:
        return repr(self.tracks)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([track.to_list() for track in self.tracks], columns=self.tracks[0].columns)


def try_make_request(url_request: request.Request, max_attempts: int = 2,
                     rate_limit_break_time: float = 2,
                     artificial_wait_time: int = 30 / 50) -> dict:
    is_rate_limited = True
    attempts = 0
    content = None

    while is_rate_limited:
        if attempts >= max_attempts:
            raise API_RateLimitError(max_attempts)

        try:
            with request.urlopen(url_request) as response:
                content = json.loads(response.read().decode("utf-8"))

        except HTTPError as e:
            headers = e.headers
            response_status = response.status

            if response_status == 429:
                is_rate_limited = True

            if response_status == 498:
                authorize_from_client_secrets()

            print(f"\nError has occurred\n{headers}")
            if "Retry-After" in headers and headers["Retry-After"] is not None:
                time.sleep(float(headers["Retry-After"]))

        attempts += 1

        if is_rate_limited:
            time.sleep(rate_limit_break_time)

        time.sleep(artificial_wait_time)

    return content


def authorize(client_id: str, client_secret: str, access_token_path: str) -> dict:
    token_url = "https://accounts.spotify.com/api/token"

    data = urllib.parse.urlencode({"grant_type": "client_credentials",
                                   "client_id": client_id,
                                   "client_secret": client_secret}).encode()

    header = {"Content-Type": "application/x-www-form-urlencoded"}

    spotify_authorisation_request = request.Request(token_url, headers=header, data=data)
    content = try_make_request(spotify_authorisation_request)

    TextWriterSingleLine(access_token_path).save(content["access_token"])
    return content


def get_spotify_track(track_id: str, access_token: str) -> dict:
    track_url = f"https://api.spotify.com/v1/tracks/{track_id}?market=GB"

    headers = {
        "redirect_uri": "data-analysis-mn-login://callback",
        "Authorization": f"Bearer {access_token}"
    }

    spotify_request = request.Request(track_url, headers=headers)

    content = try_make_request(spotify_request)
    return content


def get_spotify_tracks(track_ids: list[str], access_token: str) -> dict:
    track_ids_suffix = '%2C'.join(str(x[0].split(':')[-1]) for x in track_ids[:4])
    track_url = f"https://api.spotify.com/v1/tracks?market=GB&ids={track_ids_suffix}"

    headers = {
        "redirect_uri": "data-analysis-mn-login://callback",
        "Authorization": f"Bearer {access_token}"
    }

    spotify_request = request.Request(track_url, headers=headers)
    return try_make_request(spotify_request)


def get_album(album_id: str, access_token: str) -> dict:
    album_url = f"https://api.spotify.com/v1/albums/{album_id}"

    headers = {
        "redirect_uri": "data-analysis-mn-login://callback",
        "Authorization": f"Bearer {access_token}"
    }

    spotify_request = request.Request(album_url, headers=headers)
    return try_make_request(spotify_request)


def parse_tracks(tracks_info: list[dict], album_name: str, album_id: str) -> list[TrackMinimal]:
    tracks = []
    for track in tracks_info:
        artists = [Artist(artist["name"], artist["id"]) for artist in track["artists"]]

        duration = track["duration_ms"]

        tracks.append(TrackMinimal(track["name"], artists, track["id"],
                                   album_name, album_id,
                                   duration, track["is_local"]))

    return tracks


def download_album_metadata(track_id: str, access_token: str) -> pd.DataFrame:
    track_data = get_spotify_track(track_id, access_token)

    album_id = track_data["album"]["id"]
    album_name = track_data["album"]["name"]

    album_data = get_album(album_id, access_token)

    tracks = Tracks(parse_tracks(album_data["tracks"]["items"], album_name, album_id))

    data = tracks.to_dataframe()
    return data


def download_albums_metadata(track_ids: list[str], access_token: str) -> pd.DataFrame:
    if len(track_ids) > API_MAX_TRACK_IDS_BATCH_SIZE:
        raise API_BatchSizeTooLargeError(len(track_ids), API_MAX_TRACK_IDS_BATCH_SIZE)

    tracks_data = get_spotify_tracks(track_ids, access_token)["tracks"]

    data_all = track_response_to_dataframe(tracks_data[0], access_token)

    for track_data in tracks_data[1:]:
        data_all = pd.concat([data_all, track_response_to_dataframe(track_data, access_token)],
                             ignore_index=True)

    return data_all


def track_response_to_dataframe(track_data: dict, access_token: str) -> pd.DataFrame:
    album_id = track_data["album"]["id"]
    album_name = track_data["album"]["name"]

    album_data = get_album(album_id, access_token)

    tracks = Tracks(parse_tracks(album_data["tracks"]["items"], album_name, album_id))

    data = tracks.to_dataframe()
    return data


def save_album_for_track_id(track_id: str, sql_connection, access_token: str) -> None:
    print(f"Recording the tracks for album that contains song id={track_id}")
    data = download_album_metadata(track_id, access_token)
    save_to_database_track_metadata(data, sql_connection)
    return None


def save_to_database_track_metadata(data: pd.DataFrame, sql_connection) -> None:
    cursor = sql_connection.cursor()
    columns = data.columns
    insert_sql_statement = (f"WITH cte "
                            f"AS "
                            f"( "
                            f"SELECT ? name, ? main_artist, ? track_id, ? album_name, ? album_id, ? duration_ms, ? is_local "
                            f") "
                            f"INSERT INTO tracks ({', '.join(columns)}) "
                            f"SELECT {', '.join(columns)} FROM cte "
                            f"WHERE NOT EXISTS (SELECT * FROM tracks WHERE tracks.track_id = cte.track_id)")
    for index, row in data.iterrows():
        cursor.execute(insert_sql_statement, *row.tolist())
    sql_connection.commit()
    cursor.close()
    return None


def save_album_for_track_ids(track_ids: list[str], sql_connection, access_token: str,
                             latest_recorded_row_path: str,
                             batch_size: int=API_MAX_TRACK_IDS_BATCH_SIZE) -> None:
    if batch_size > API_MAX_TRACK_IDS_BATCH_SIZE:
        raise API_BatchSizeTooLargeError(batch_size, API_MAX_TRACK_IDS_BATCH_SIZE)

    latest_recorded_row_writer = TextWriterSingleLine(latest_recorded_row_path)

    latest_row_number = int(latest_recorded_row_writer.load())

    for track_ids_batch in batched(track_ids, batch_size):
        print("Downloading a batch of songs...")
        data = download_albums_metadata(track_ids_batch, access_token=access_token)
        print(f"Found {data.shape[0]} songs")
        save_to_database_track_metadata(data, sql_connection)

        latest_row_number += len(track_ids_batch)
        latest_recorded_row_writer.save(str(latest_row_number))

    return None


def authorize_from_client_secrets() -> None:
    client_id = TextWriterSingleLine("client_id.txt").load()
    client_secret = TextWriterSingleLine("client_secret.txt").load()

    authorize(client_id, client_secret, ACCESS_TOKEN_PATH)
    return None


def main() -> None:
    authorize_from_client_secrets()

    latest_recorded_row = int(TextWriterSingleLine("latest_recorded_track_row_number.txt").load())

    track_ids = pd.read_csv("Spotify History Audio/all_track_ids.csv").to_numpy().tolist()

    if latest_recorded_row >= len(track_ids):
        print("Finished recording all track ids!")
        return None

    connection = pyodbc.connect(driver='{SQL Server}',
                                server=SERVER,
                                database=DATABASE,
                                trusted_connection='yes')

    access_token = TextWriterSingleLine(ACCESS_TOKEN_PATH).load()
    save_album_for_track_ids(track_ids=track_ids[latest_recorded_row:],
                             sql_connection=connection, access_token=access_token,
                             latest_recorded_row_path="latest_recorded_track_row_number.txt")


if __name__ == "__main__":
    main()
