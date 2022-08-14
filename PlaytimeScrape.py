from requests import session, Session
from dotenv import dotenv_values
import psycopg
from psycopg_pool import ConnectionPool
import logging
import schedule
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Literal, Any, Callable


class PlaytimeScrape:
    def __init__(self):
        logging.info("class instantiated")
        self.env: dict[str, str] = dotenv_values(".env")

        self.urls: dict[str, bool] = {
            "http://v1.api.tycoon.community/main": False,
            "http://v1.api.tycoon.community/beta": False,
        }

        self.session: Session = session()
        self.session.headers.update({"x-tycoon-key": self.env["tycoon"]})

        self.pool = ConnectionPool(
            conninfo=f"dbname={self.env['postgresdb']} user={self.env['postgresuser']} password={self.env['postgrespassword']}", open=True
        )

        logging.debug(
            f"env: {self.env}\n\nsession: {self.session}\n\ndb pool: {self.pool}"
        )

        with self.pool.connection() as connection:  # type:psycopg.Connection
            connection.execute(
                "CREATE TABLE IF NOT EXISTS serveractivity(server TEXT, players INT, timestamp TIMESTAMP)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS totalplaytime(player INT PRIMARY KEY, playtime INTERVAL, last_seen TIMESTAMP)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS playtime(player INT, name TEXT, playtime INTERVAL, timestamp DATE, PRIMARY KEY(player, timestamp))"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS uptime(server TEXT, uptime INTERVAL, timestamp DATE, PRIMARY KEY(server, timestamp))"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS aliases(player INT PRIMARY KEY, current_name TEXT, aliases TEXT[])"
            )

        self.FUNCTION: dict[str, Callable] = {
            "alive": self.alive,
            "players": self.players,
        }

        logging.info("Setup complete")

    def executor(self, function: Literal["alive", "players"], *args, **kwargs) -> None:
        with ThreadPoolExecutor(max_workers=5) as executor:
            logging.debug(f"{str(function)}, {args}, {kwargs}")
            executor.submit(self.FUNCTION[function], *args, **kwargs)

    def get(self, server: str, endpoint: str) -> dict | bool:
        try:
            request = self.session.get(url=f"{server}/{endpoint}", timeout=10)
        except Exception as e:
            logging.debug(e)
            return False
        else:
            match request.status_code:
                case 200:
                    logging.debug(200)
                    return request.json()
                case 204:
                    logging.debug(204)
                    return True
                case _:
                    logging.debug(request.status_code)
                    return False

    def submit(self, query: str, variables: dict[str, Any]) -> None:
        with self.pool.connection() as connection:  # type: psycopg.Connection
            connection.execute(query, variables)

    def alive(self) -> None:
        for server in self.urls:
            self.urls[server] = self.get(server, "alive")
            if self.urls[server]:
                self.submit(
                    "INSERT INTO uptime(server, uptime, timestamp) VALUES(%(server)s, '5 seconds'::INTERVAL, now()::DATE) ON CONFLICT(server, timestamp) DO UPDATE SET uptime = uptime.uptime + '5 seconds'::INTERVAL;",
                    {"server": server.split("/")[-1]},
                )
        logging.debug(self.urls)

    def players(self) -> None:
        for server, status in self.urls.items():
            if status is False:
                logging.debug("server ofline")
                continue
            players = self.get(server, "players.json")
            logging.debug(players)
            if not players.get("players"):
                logging.debug("no players")
                self.submit(
                    "INSERT INTO serveractivity(server, players, timestamp) VALUES(%(server)s, 0, now()::TIMESTAMP);",
                    {"server": server.split("/")[-1]},
                )
                continue
            for player in players.get("players"):
                logging.debug(player)
                self.submit(
                    "INSERT INTO playtime(player, name, playtime, timestamp) VALUES(%(player)s, %(name)s, '1 minute'::interval, now()::DATE) ON CONFLICT(player, timestamp) DO UPDATE SET playtime = playtime.playtime + '1 minute'::INTERVAL;",
                    {"player": player[2], "name": player[0]},
                )
                self.submit(
                    "INSERT INTO totalplaytime(player, playtime, last_seen) VALUES (%(player)s, '1 minute'::interval, now():: timestamp) ON CONFLICT(player) DO UPDATE SET playtime = totalplaytime.playtime + '1 minute'::INTERVAL, last_seen = excluded.last_seen;",
                    {"player": player[2]},
                )
                self.submit(
                    "INSERT INTO aliases(player, current_name, aliases) VALUES (%(player)s, %(name)s, ARRAY [%(name)s]) ON CONFLICT(player) DO UPDATE SET aliases = ARRAY (SELECT DISTINCT unnest(aliases.aliases || excluded.current_name)), current_name = excluded.current_name",
                    {"player": player[2], "name": player[0]},
                )
            self.submit(
                "INSERT INTO serveractivity(server, players, timestamp) VALUES(%(server)s, %(players)s, now()::TIMESTAMP);",
                {
                    "server": server.split("/")[-1],
                    "players": len(players.get("players")),
                },
            )

    def main(self) -> None:
        schedule.every(5).seconds.do(self.executor, "alive")
        schedule.every(1).minute.do(self.executor, "players")
        logging.info(schedule.jobs)
        while True:
            schedule.run_pending()
            sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y/%m/%d %H:%M:%S",
    )
    PlaytimeScrape().main()
