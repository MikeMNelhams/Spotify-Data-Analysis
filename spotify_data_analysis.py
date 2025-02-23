import json
import pandas as pd

from typing import Any, Self

import os

from file_operations_mn.file_writers import TextWriterSingleLine
from file_operations_mn.path_string_utilities import file_path_without_extension, is_path_of_extension
from file_operations_mn.file_utilities_read import file_exists
from file_operations_mn.file_exceptions import InvalidPathError


class Listen:
    def __init__(self, ts: str, platform: str, ms_played: int, conn_country: str, master_metadata_track_name: str,
                 master_metadata_album_artist_name: str, master_metadata_album_album_name: str,
                 spotify_track_uri: str, episode_name: str | None, episode_show_name: str | None,
                 spotify_episode_uri: str | None, audiobook_title: str | None, audiobook_uri: str | None,
                 audiobook_chapter_uri: str | None, audiobook_chapter_title: str | None,
                 reason_start: str, reason_end: str, shuffle: bool, skipped: bool, offline: bool,
                 offline_timestamp: str | None, incognito_mode: bool):
        self.ts = ts
        self.platform = platform
        self.ms_played = ms_played
        self.conn_country = conn_country
        self.master_metadata_track_name = master_metadata_track_name
        self.master_metadata_album_artist_name = master_metadata_album_artist_name
        self.master_metadata_album_album_name = master_metadata_album_album_name
        self.spotify_track_uri = spotify_track_uri
        self.episode_name = "" if episode_name is None else episode_name
        self.episode_show_name = "" if episode_show_name is None else episode_show_name
        self.spotify_episode_uri = "" if spotify_episode_uri is None else spotify_episode_uri
        self.audiobook_title = "" if audiobook_title is None else audiobook_title
        self.audiobook_uri = "" if audiobook_uri is None else audiobook_uri
        self.audiobook_chapter_uri = "" if audiobook_chapter_uri is None else audiobook_chapter_uri
        self.audiobook_chapter_title = "" if audiobook_chapter_title is None else audiobook_chapter_title
        self.reason_start = reason_start
        self.reason_end = reason_end
        self.shuffle = shuffle
        self.skipped = skipped
        self.offline = offline
        self.offline_timestamp = "" if offline_timestamp is None else offline_timestamp
        self.incognito_mode = incognito_mode

    def __repr__(self) -> str:
        minutes_played = self.ms_played / (1000 * 60)
        minutes_truncated = int(minutes_played)

        remaining_seconds = (minutes_played - minutes_truncated) * 60
        remaining_seconds_string = str(int(round(remaining_seconds, 0))).zfill(2)

        return (f"Listen[({self.master_metadata_track_name} by {self.master_metadata_album_artist_name}): "
                f"{self.ts} for {minutes_truncated}:{remaining_seconds_string}]")

    @classmethod
    def from_dict(cls, listen: dict[str, Any]) -> Self:
        listen.pop("ip_addr")
        return cls(**listen)

    @property
    def to_list(self) -> list[str | int | bool]:
        return [self.ts, self.platform, self.ms_played, self.conn_country,
                self.master_metadata_track_name, self.master_metadata_album_artist_name,
                self.master_metadata_album_album_name, self.spotify_track_uri, self.episode_name,
                self.episode_show_name, self.spotify_episode_uri, self.audiobook_title, self.audiobook_uri,
                self.audiobook_chapter_uri, self.audiobook_chapter_title, self.reason_start, self.reason_end,
                self.shuffle, self.skipped, self.offline, self.offline_timestamp, self.incognito_mode]

    @property
    def column_names(self) -> list[str]:
        return ["ts", "platform", "ms_played", "conn_country",
                "master_metadata_track_name", "master_metadata_album_artist_name",
                "master_metadata_album_album_name", "spotify_track_uri", "episode_name",
                "episode_show_name", "spotify_episode_uri", "audiobook_title", "audiobook_uri",
                "audiobook_chapter_uri", "audiobook_chapter_title", "reason_start", "reason_end",
                "shuffle", "skipped", "offline", "offline_timestamp", "incognito_mode"]


def streaming_history_audio_json_to_dataframe(path: str) -> pd.DataFrame:
    if not is_path_of_extension(path, ".json"):
        raise InvalidPathError(f"Path: {path} is not a .json file!")

    with open(path, 'r', encoding="utf-8") as file:
        content = json.load(file)

    listens = [Listen.from_dict(listen) for listen in content]
    data = pd.DataFrame(tuple(listen.to_list for listen in listens), columns=listens[0].column_names)
    return data


def streaming_history_audio_to_csv(source_path: str, save_path: str) -> None:
    if not is_path_of_extension(source_path, '.json'):
        raise InvalidPathError(f"Source path: {source_path} is not a .json file!")

    if not is_path_of_extension(save_path, '.csv'):
        raise InvalidPathError(f"Save path: {save_path} is not a .csv file!")

    data = streaming_history_audio_json_to_dataframe(source_path)
    data.to_csv(save_path, sep=',')
    return None


def streaming_history_audios_to_csv(data_folder: str, save_path_prefix: str) -> None:
    for _, _, files in os.walk(data_folder, topdown=True):
        for file_suffix in files:
            print(file_suffix)
            source_path = data_folder + file_suffix
            if not is_path_of_extension(source_path, '.json'):
                continue

            if "Video" in source_path:
                continue

            year_suffix = file_path_without_extension('_'.join(file_suffix.split('_')[-3:]))
            save_path = f"{save_path_prefix}{year_suffix}.csv"

            if file_exists(save_path):
                raise FileExistsError(f"File path: {save_path} already exists."
                                      f"\nDelete the file if you want to save the data.")

            print(f"Saving .csv to {save_path}")

            streaming_history_audio_to_csv(source_path, save_path)

    return None


def append_all_to_csv(directory_path: str) -> None:
    if directory_path[-1] != '/':
        directory_path += '/'

    data_file_paths = []

    for _, _, files in os.walk(directory_path, topdown=True):
        for file_suffix in files:
            source_path = directory_path + file_suffix
            if not is_path_of_extension(source_path, '.csv'):
                continue

            data_file_paths.append(source_path)

    if not data_file_paths:
        raise FileNotFoundError("No .csv files found")

    data_all = pd.read_csv(data_file_paths[0], index_col=0)

    for source_path in data_file_paths[1:]:
        print(f"reading: {source_path}")
        data = pd.read_csv(source_path, index_col=0)
        data_all = pd.concat((data_all, data))

    print(f"Appending all to {directory_path}all.csv")
    data_all.to_csv(f"{directory_path}all.csv")
    return None


def main():
    username = TextWriterSingleLine("current_user.txt").load()
    streaming_history_audios_to_csv(f"Spotify Extended Streaming History/{username}/",
                                    f"Spotify History Audio/{username}/")

    append_all_to_csv(f"Spotify History Audio/{username}/")


if __name__ == "__main__":
    main()
